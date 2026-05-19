<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLCache;

use Closure;
use Flow\Bridge\Symfony\PostgreSQLCache\Exception\CacheException;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\ConnectionParameters;
use Flow\PostgreSql\Client\Types\ValueType;
use LogicException;
use Symfony\Component\Cache\Adapter\AbstractAdapter;
use Symfony\Component\Cache\Exception\InvalidArgumentException;
use Symfony\Component\Cache\Marshaller\DefaultMarshaller;
use Symfony\Component\Cache\Marshaller\MarshallerInterface;
use Symfony\Component\Cache\PruneableInterface;

use function array_merge;
use function array_values;
use function Flow\PostgreSql\DSL\and_;
use function Flow\PostgreSql\DSL\binary_expr;
use function Flow\PostgreSql\DSL\case_when;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\conflict_columns;
use function Flow\PostgreSql\DSL\delete;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\gt;
use function Flow\PostgreSql\DSL\in_;
use function Flow\PostgreSql\DSL\insert;
use function Flow\PostgreSql\DSL\is_null;
use function Flow\PostgreSql\DSL\le;
use function Flow\PostgreSql\DSL\like;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\on_conflict_update;
use function Flow\PostgreSql\DSL\or_;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\pgsql_client;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;
use function Flow\PostgreSql\DSL\truncate_table;
use function Flow\PostgreSql\DSL\typed;
use function Flow\PostgreSql\DSL\when;
use function get_debug_type;
use function is_string;
use function preg_match;
use function sprintf;
use function time;

final class FlowPostgreSqlCacheAdapter extends AbstractAdapter implements PruneableInterface
{
    private const int MAX_KEY_LENGTH = 255;

    private ?Client $client;

    private readonly Closure $clientFactory;

    private readonly ?ConnectionParameters $connectionParameters;

    private readonly string $dataCol;

    private readonly string $idCol;

    private readonly string $lifetimeCol;

    private readonly MarshallerInterface $marshaller;

    private readonly string $poolNamespace;

    private readonly string $schema;

    private readonly string $table;

    private readonly string $timeCol;

    /**
     * @param array{db_table?: string, db_schema?: string, db_id_col?: string, db_data_col?: string, db_lifetime_col?: string, db_time_col?: string} $options
     * @param ?\Closure(ConnectionParameters): Client $clientFactory Internal — for tests. Defaults to `pgsql_client(...)`.
     */
    public function __construct(
        ConnectionParameters|Client $connection,
        string $namespace = '',
        int $defaultLifetime = 0,
        array $options = [],
        ?MarshallerInterface $marshaller = null,
        ?Closure $clientFactory = null,
    ) {
        if (isset($namespace[0]) && preg_match('#[^-+.A-Za-z0-9]#', $namespace, $match)) {
            throw new InvalidArgumentException(sprintf(
                'Namespace contains "%s" but only characters in [-+.A-Za-z0-9] are allowed.',
                $match[0],
            ));
        }

        if ($connection instanceof Client) {
            $this->client = $connection;
            $this->connectionParameters = null;
        } else {
            $this->client = null;
            $this->connectionParameters = $connection;
        }

        $this->maxIdLength = self::MAX_KEY_LENGTH;
        $this->poolNamespace = $namespace;
        $this->table = $options['db_table'] ?? 'cache_items';
        $this->schema = $options['db_schema'] ?? 'public';
        $this->idCol = $options['db_id_col'] ?? 'item_id';
        $this->dataCol = $options['db_data_col'] ?? 'item_data';
        $this->lifetimeCol = $options['db_lifetime_col'] ?? 'item_lifetime';
        $this->timeCol = $options['db_time_col'] ?? 'item_time';
        $this->marshaller = $marshaller ?? new DefaultMarshaller();
        $this->clientFactory =
            $clientFactory ?? static fn(ConnectionParameters $params): Client => pgsql_client($params);

        parent::__construct($namespace, $defaultLifetime);
    }

    public function prune(): bool
    {
        $conditions = [
            is_null(col($this->lifetimeCol), not: true),
            le(binary_expr(col($this->lifetimeCol), '+', col($this->timeCol)), param(1)),
        ];
        $parameters = [time()];

        if ($this->poolNamespace !== '') {
            $conditions[] = like(col($this->idCol), param(2));
            $parameters[] = $this->poolNamespace . '%';
        }

        $this->client()->execute(
            delete()->from(table($this->table, $this->schema))->where(and_(...$conditions)),
            $parameters,
        );

        return true;
    }

    protected function doClear(string $namespace): bool
    {
        if ($namespace === '') {
            $this->client()->execute(truncate_table($this->schema . '.' . $this->table));

            return true;
        }

        $this->client()->execute(
            delete()->from(table($this->table, $this->schema))->where(like(col($this->idCol), param(1))),
            [$namespace . '%'],
        );

        return true;
    }

    /**
     * @param array<int, string> $ids
     */
    protected function doDelete(array $ids): bool
    {
        if ($ids === []) {
            return true;
        }

        $values = array_values($ids);
        $placeholders = [];
        $position = 1;

        foreach ($values as $_) {
            $placeholders[] = param($position++);
        }

        $this->client()->execute(
            delete()->from(table($this->table, $this->schema))->where(in_(col($this->idCol), $placeholders)),
            $values,
        );

        return true;
    }

    /**
     * @param array<int, string> $ids
     *
     * @return iterable<string, mixed>
     */
    protected function doFetch(array $ids): iterable
    {
        if ($ids === []) {
            return;
        }

        $now = time();
        $values = array_values($ids);

        $placeholders = [];
        $position = 2;

        foreach ($values as $_) {
            $placeholders[] = param($position++);
        }

        $dataExpression = case_when([
            when(
                or_(
                    is_null(col($this->lifetimeCol)),
                    gt(binary_expr(col($this->lifetimeCol), '+', col($this->timeCol)), param(1)),
                ),
                col($this->dataCol),
            ),
        ], elseResult: literal(null));

        $rows = $this->client()->fetchAll(
            select(col($this->idCol), $dataExpression->as($this->dataCol))
                ->from(table($this->table, $this->schema))
                ->where(in_(col($this->idCol), $placeholders)),
            array_merge([$now], $values),
        );

        $expired = [];

        foreach ($rows as $row) {
            $rowId = $row[$this->idCol];
            $rowData = $row[$this->dataCol];

            if (!is_string($rowId)) {
                throw CacheException::unexpectedRowShape($this->idCol, get_debug_type($rowId));
            }

            if ($rowData === null) {
                $expired[] = $rowId;

                continue;
            }

            if (!is_string($rowData)) {
                throw CacheException::unexpectedRowShape($this->dataCol, get_debug_type($rowData));
            }

            yield $rowId => $this->marshaller->unmarshall($rowData);
        }

        if ($expired !== []) {
            $this->doDelete($expired);
        }
    }

    protected function doHave(string $id): bool
    {
        $row = $this->client()->fetch(
            select(literal(1))
                ->from(table($this->table, $this->schema))
                ->where(and_(
                    eq(col($this->idCol), param(1)),
                    or_(
                        is_null(col($this->lifetimeCol)),
                        gt(binary_expr(col($this->lifetimeCol), '+', col($this->timeCol)), param(2)),
                    ),
                ))
                ->limit(1),
            [$id, time()],
        );

        return $row !== null;
    }

    /**
     * @param array<string, mixed> $values
     *
     * @return array<int, string>
     */
    protected function doSave(array $values, int $lifetime): array
    {
        $failed = [];
        $marshalled = $this->marshaller->marshall($values, $failed);

        if ($marshalled === []) {
            return $failed ?? [];
        }

        $this->client()->transaction(function (Client $client) use ($marshalled, $lifetime): void {
            $now = time();
            $expiry = $lifetime > 0 ? $lifetime : null;

            foreach ($marshalled as $id => $data) {
                $client->execute(
                    insert()
                        ->into(table($this->table, $this->schema))
                        ->columns($this->idCol, $this->dataCol, $this->lifetimeCol, $this->timeCol)
                        ->values(param(1), param(2), param(3), param(4))
                        ->onConflict(on_conflict_update(conflict_columns([$this->idCol]), [
                            $this->dataCol => col($this->dataCol, 'excluded'),
                            $this->lifetimeCol => col($this->lifetimeCol, 'excluded'),
                            $this->timeCol => col($this->timeCol, 'excluded'),
                        ])),
                    [
                        (string) $id,
                        typed($data, ValueType::BYTEA),
                        $expiry,
                        $now,
                    ],
                );
            }
        });

        return $failed ?? [];
    }

    private function client(): Client
    {
        if ($this->client !== null) {
            return $this->client;
        }

        if ($this->connectionParameters === null) {
            throw new LogicException('FlowPostgreSqlCacheAdapter has no client and no connection parameters.');
        }

        return $this->client = ($this->clientFactory)($this->connectionParameters);
    }
}
