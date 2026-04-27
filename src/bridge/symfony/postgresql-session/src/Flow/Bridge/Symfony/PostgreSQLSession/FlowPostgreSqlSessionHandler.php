<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLSession;

use function Flow\PostgreSql\DSL\{col, conflict_columns, delete, eq, func, insert, lt, on_conflict_update, param, pgsql_client, select, table, truncate_table, typed, update};

use Flow\Bridge\Symfony\PostgreSQLSession\Exception\SessionException;
use Flow\PostgreSql\Client\{Client, ConnectionParameters};
use Flow\PostgreSql\Client\Types\ValueType;
use Symfony\Component\HttpFoundation\Session\Storage\Handler\AbstractSessionHandler;

class FlowPostgreSqlSessionHandler extends AbstractSessionHandler
{
    public const int LOCK_ADVISORY = 1;

    public const int LOCK_NONE = 0;

    public const int LOCK_TRANSACTIONAL = 2;

    private ?Client $client;

    private readonly \Closure $clientFactory;

    private readonly ?ConnectionParameters $connectionParameters;

    private readonly string $dataCol;

    private bool $gcCalled = false;

    private readonly string $idCol;

    private readonly string $lifetimeCol;

    /**
     * @var array<string, int>
     */
    private array $lockedSessions = [];

    private readonly int $lockMode;

    private readonly string $schema;

    private readonly string $table;

    private readonly string $timeCol;

    private bool $transactionOpen = false;

    private readonly ?int $ttl;

    /**
     * @param array{
     *     db_table?: string,
     *     db_schema?: string,
     *     db_id_col?: string,
     *     db_data_col?: string,
     *     db_lifetime_col?: string,
     *     db_time_col?: string,
     *     lock_mode?: int,
     *     ttl?: null|int,
     * } $options
     * @param ?\Closure(ConnectionParameters): Client $clientFactory Internal — for tests. Defaults to `pgsql_client(...)`.
     */
    public function __construct(
        ConnectionParameters|Client $connection,
        array $options = [],
        ?\Closure $clientFactory = null,
    ) {
        if ($connection instanceof Client) {
            $this->client = $connection;
            $this->connectionParameters = null;
        } else {
            $this->client = null;
            $this->connectionParameters = $connection;
        }

        $this->table = $options['db_table'] ?? 'sessions';
        $this->schema = $options['db_schema'] ?? 'public';
        $this->idCol = $options['db_id_col'] ?? 'sess_id';
        $this->dataCol = $options['db_data_col'] ?? 'sess_data';
        $this->lifetimeCol = $options['db_lifetime_col'] ?? 'sess_lifetime';
        $this->timeCol = $options['db_time_col'] ?? 'sess_time';
        $this->ttl = $options['ttl'] ?? null;
        $this->clientFactory = $clientFactory ?? static fn (ConnectionParameters $params) : Client => pgsql_client($params);

        $lockMode = $options['lock_mode'] ?? self::LOCK_TRANSACTIONAL;

        if (!\in_array($lockMode, [self::LOCK_NONE, self::LOCK_ADVISORY, self::LOCK_TRANSACTIONAL], true)) {
            throw new \InvalidArgumentException(\sprintf(
                'Invalid lock_mode "%s". Use one of FlowPostgreSqlSessionHandler::LOCK_NONE, LOCK_ADVISORY, LOCK_TRANSACTIONAL.',
                \is_scalar($lockMode) ? (string) $lockMode : \get_debug_type($lockMode),
            ));
        }

        $this->lockMode = $lockMode;
    }

    public function close() : bool
    {
        if ($this->client === null && !$this->gcCalled) {
            return true;
        }

        $this->releaseAdvisoryLocks();

        if ($this->transactionOpen) {
            $this->client()->commit();
            $this->transactionOpen = false;
        }

        if ($this->gcCalled) {
            $this->gcCalled = false;

            $this->client()->execute(
                delete()
                    ->from(table($this->table, $this->schema))
                    ->where(lt(col($this->lifetimeCol), param(1))),
                [\time()],
            );
        }

        if ($this->client !== null && $this->connectionParameters !== null) {
            $this->client->close();
            $this->client = null;
        }

        return true;
    }

    public function gc(int $max_lifetime) : int|false
    {
        $this->gcCalled = true;

        return 0;
    }

    public function purgeAll() : int
    {
        $this->client()->execute(truncate_table($this->schema . '.' . $this->table));

        return 0;
    }

    public function purgeExpired() : int
    {
        return $this->client()->execute(
            delete()
                ->from(table($this->table, $this->schema))
                ->where(lt(col($this->lifetimeCol), param(1))),
            [\time()],
        );
    }

    /**
     * Override AbstractSessionHandler::updateTimestamp so that touching a
     * session (without changing its data) refreshes the lifetime/time columns.
     * Otherwise long-lived sessions whose payload never changes would be
     * garbage-collected even if still in active use.
     */
    #[\Override]
    public function updateTimestamp(#[\SensitiveParameter] string $sessionId, string $data) : bool
    {
        $now = \time();
        $expiry = $now + $this->resolveTtl();

        $this->client()->execute(
            update()
                ->update(table($this->table, $this->schema))
                ->set($this->lifetimeCol, param(1))
                ->set($this->timeCol, param(2))
                ->where(eq(col($this->idCol), param(3))),
            [$expiry, $now, $sessionId],
        );

        $this->commitTransactionalLock();
        $this->releaseLockFor($sessionId);

        return true;
    }

    protected function doDestroy(#[\SensitiveParameter] string $sessionId) : bool
    {
        $this->client()->execute(
            delete()
                ->from(table($this->table, $this->schema))
                ->where(eq(col($this->idCol), param(1))),
            [$sessionId],
        );

        $this->commitTransactionalLock();
        $this->releaseLockFor($sessionId);

        return true;
    }

    protected function doRead(#[\SensitiveParameter] string $sessionId) : string
    {
        $this->acquireLockFor($sessionId);

        $select = select(col($this->dataCol), col($this->lifetimeCol))
            ->from(table($this->table, $this->schema))
            ->where(eq(col($this->idCol), param(1)));

        if ($this->lockMode === self::LOCK_TRANSACTIONAL) {
            $select = $select->forUpdate();
        }

        $row = $this->client()->fetch($select, [$sessionId]);

        if ($row === null) {
            return '';
        }

        $lifetime = $row[$this->lifetimeCol];

        if (!\is_int($lifetime)) {
            throw SessionException::unexpectedRowShape($this->lifetimeCol, \get_debug_type($lifetime));
        }

        if ($lifetime < \time()) {
            return '';
        }

        $data = $row[$this->dataCol];

        if (\is_resource($data)) {
            $contents = \stream_get_contents($data);

            if ($contents === false) {
                throw SessionException::unexpectedRowShape($this->dataCol, 'resource (unreadable)');
            }

            return $contents;
        }

        if (!\is_string($data)) {
            throw SessionException::unexpectedRowShape($this->dataCol, \get_debug_type($data));
        }

        return $data;
    }

    protected function doWrite(#[\SensitiveParameter] string $sessionId, string $data) : bool
    {
        $now = \time();
        $expiry = $now + $this->resolveTtl();

        $this->client()->execute(
            insert()
                ->into(table($this->table, $this->schema))
                ->columns($this->idCol, $this->dataCol, $this->lifetimeCol, $this->timeCol)
                ->values(param(1), param(2), param(3), param(4))
                ->onConflict(on_conflict_update(
                    conflict_columns([$this->idCol]),
                    [
                        $this->dataCol => col($this->dataCol, 'excluded'),
                        $this->lifetimeCol => col($this->lifetimeCol, 'excluded'),
                        $this->timeCol => col($this->timeCol, 'excluded'),
                    ],
                )),
            [
                $sessionId,
                typed($data, ValueType::BYTEA),
                $expiry,
                $now,
            ],
        );

        $this->commitTransactionalLock();
        $this->releaseLockFor($sessionId);

        return true;
    }

    private function acquireLockFor(string $sessionId) : void
    {
        if ($this->lockMode === self::LOCK_TRANSACTIONAL) {
            if (!$this->transactionOpen) {
                $this->client()->beginTransaction();
                $this->transactionOpen = true;
            }

            return;
        }

        if ($this->lockMode === self::LOCK_ADVISORY) {
            $key = $this->convertSessionIdToLockKey($sessionId);
            $this->client()->fetch(
                select(func('pg_advisory_lock', [param(1)])),
                [$key],
            );
            $this->lockedSessions[$sessionId] = $key;
        }
    }

    private function client() : Client
    {
        if ($this->client !== null) {
            return $this->client;
        }

        if ($this->connectionParameters === null) {
            throw new \LogicException('FlowPostgreSqlSessionHandler has no client and no connection parameters.');
        }

        return $this->client = ($this->clientFactory)($this->connectionParameters);
    }

    private function commitTransactionalLock() : void
    {
        if ($this->lockMode === self::LOCK_TRANSACTIONAL && $this->transactionOpen) {
            $this->client()->commit();
            $this->transactionOpen = false;
        }
    }

    /**
     * Convert the first 8 bytes of $sessionId into a signed bigint, matching
     * the encoding used by Symfony's PdoSessionHandler::convertStringToInt
     * so that advisory locks acquired by either handler collide as expected.
     */
    private function convertSessionIdToLockKey(string $sessionId) : int
    {
        $padded = \str_pad($sessionId, 8, "\0");

        $int1 = (\ord($padded[7]) << 24)
            + (\ord($padded[6]) << 16)
            + (\ord($padded[5]) << 8)
            + \ord($padded[4]);
        $int2 = (\ord($padded[3]) << 24)
            + (\ord($padded[2]) << 16)
            + (\ord($padded[1]) << 8)
            + \ord($padded[0]);

        return $int2 + ($int1 << 32);
    }

    private function releaseAdvisoryLocks() : void
    {
        if ($this->lockMode !== self::LOCK_ADVISORY) {
            return;
        }

        foreach ($this->lockedSessions as $key) {
            $this->client()->fetch(
                select(func('pg_advisory_unlock', [param(1)])),
                [$key],
            );
        }

        $this->lockedSessions = [];
    }

    private function releaseLockFor(string $sessionId) : void
    {
        if ($this->lockMode === self::LOCK_ADVISORY && \array_key_exists($sessionId, $this->lockedSessions)) {
            $key = $this->lockedSessions[$sessionId];
            unset($this->lockedSessions[$sessionId]);
            $this->client()->fetch(
                select(func('pg_advisory_unlock', [param(1)])),
                [$key],
            );
        }
    }

    private function resolveTtl() : int
    {
        if ($this->ttl !== null) {
            return $this->ttl;
        }

        $iniValue = (int) \ini_get('session.gc_maxlifetime');

        return $iniValue > 0 ? $iniValue : 1440;
    }
}
