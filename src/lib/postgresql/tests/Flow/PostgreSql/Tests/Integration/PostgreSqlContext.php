<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration;

use Flow\PostgreSql\Client\Client;
use PgSql\Connection;
use RuntimeException;

use function Flow\PostgreSql\DSL\and_;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\drop;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\func;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\notify;
use function Flow\PostgreSql\DSL\pgsql_client;
use function Flow\PostgreSql\DSL\pgsql_connection_dsn;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;
use function getenv;
use function pg_close;
use function pg_connect;
use function pg_get_result;
use function pg_last_error;
use function pg_result_error;
use function pg_send_query;
use function sprintf;

use const PGSQL_CONNECT_FORCE_NEW;

final class PostgreSqlContext
{
    public readonly Client $client;

    /** @var list<Connection> */
    private array $backgroundConnections = [];

    private readonly string $dsn;

    /** @var list<Client> */
    private array $secondaryClients = [];

    public function __construct()
    {
        $dsn = getenv('PGSQL_DATABASE_URL');

        if (!$dsn) {
            throw new RuntimeException('PGSQL_DATABASE_URL environment variable is not set');
        }

        $this->dsn = $dsn;
        $this->client = pgsql_client(pgsql_connection_dsn($dsn));
    }

    /**
     * Drains the results of every scheduled notification and returns the errors
     * PostgreSQL reported for them. Empty when all of them succeeded.
     *
     * @return list<string>
     */
    public function backgroundNotifierErrors(): array
    {
        $errors = [];

        foreach ($this->backgroundConnections as $connection) {
            while (($result = pg_get_result($connection)) !== false) {
                $error = pg_result_error($result);

                if ($error !== false && $error !== '') {
                    $errors[] = $error;
                }
            }
        }

        return $errors;
    }

    public function client(): Client
    {
        return $this->client;
    }

    public function close(): void
    {
        foreach ($this->backgroundConnections as $connection) {
            pg_close($connection);
        }
        $this->backgroundConnections = [];

        foreach ($this->secondaryClients as $client) {
            $client->close();
        }
        $this->secondaryClients = [];

        $this->client->close();
    }

    /**
     * Reads the raw default expression stored in pg_attrdef for a column,
     * including the implicit type cast PostgreSQL keeps (e.g. '0'::numeric).
     * The column must have a stored default.
     */
    public function columnDefaultExpression(string $schema, string $table, string $column): string
    {
        return $this->client->fetchScalarString(
            select(func('pg_catalog.pg_get_expr', [col('adbin', 'd'), col('adrelid', 'd')]))
                ->from(table('pg_attrdef', 'pg_catalog')->as('d'))
                ->join(
                    table('pg_attribute', 'pg_catalog')->as('a'),
                    and_(eq(col('attrelid', 'a'), col('adrelid', 'd')), eq(col('attnum', 'a'), col('adnum', 'd'))),
                )
                ->join(table('pg_class', 'pg_catalog')->as('c'), eq(col('oid', 'c'), col('adrelid', 'd')))
                ->join(table('pg_namespace', 'pg_catalog')->as('n'), eq(col('oid', 'n'), col('relnamespace', 'c')))
                ->where(and_(
                    eq(col('relname', 'c'), literal($table)),
                    eq(col('nspname', 'n'), literal($schema)),
                    eq(col('attname', 'a'), literal($column)),
                )),
        );
    }

    public function dropDomainIfExists(string $domain): void
    {
        $this->client->execute(drop()->domain($domain)->ifExists()->cascade()->toSql());
    }

    public function dropFunctionIfExists(string $function): void
    {
        $this->client->execute(drop()->function($function)->ifExists()->cascade()->toSql());
    }

    public function dropIndexIfExists(string $index): void
    {
        $this->client->execute(drop()->index($index)->ifExists()->cascade()->toSql());
    }

    public function dropMaterializedViewIfExists(string $view): void
    {
        $this->client->execute(drop()->materializedView($view)->ifExists()->cascade()->toSql());
    }

    public function dropSchemaIfExists(string $schema): void
    {
        $this->client->execute(drop()->schema($schema)->ifExists()->cascade()->toSql());
    }

    public function dropSequenceIfExists(string $sequence): void
    {
        $this->client->execute(drop()->sequence($sequence)->ifExists()->cascade()->toSql());
    }

    public function dropTableIfExists(string $table): void
    {
        $this->client->execute(drop()->table($table)->ifExists()->cascade()->toSql());
    }

    public function dropTriggerIfExists(string $trigger, string $table): void
    {
        $this->client->execute(drop()->trigger($trigger)->ifExists()->on($table)->cascade()->toSql());
    }

    public function dropTypeIfExists(string $type): void
    {
        $this->client->execute(drop()->type($type)->ifExists()->cascade()->toSql());
    }

    public function dropViewIfExists(string $view): void
    {
        $this->client->execute(drop()->view($view)->ifExists()->cascade()->toSql());
    }

    /**
     * Opens an additional independent Client connected to the same database.
     * Used by tests that need two sessions (e.g. LISTEN/NOTIFY between
     * separate connections). The context owns the returned client and closes
     * it during tearDown.
     */
    public function newClient(): Client
    {
        $client = pgsql_client(pgsql_connection_dsn($this->dsn));
        $this->secondaryClients[] = $client;

        return $client;
    }

    /**
     * Fires a NOTIFY on $channel with $payload after $delayMs on a dedicated
     * connection, without blocking the caller. The delay runs server side, so
     * the notification arrives while the caller sits in a blocking wait on its
     * own connection. Used by tests that exercise the blocking wait path of
     * waitForNotification().
     */
    public function scheduleNotify(string $channel, string $payload, int $delayMs): void
    {
        $connection = pg_connect(pgsql_connection_dsn($this->dsn)->toString(), PGSQL_CONNECT_FORCE_NEW);

        if ($connection === false) {
            throw new RuntimeException('Failed to open a connection for the scheduled notification');
        }

        $this->backgroundConnections[] = $connection;

        $sent = pg_send_query($connection, sprintf(
            '%s; %s',
            select(func('pg_sleep', [literal($delayMs / 1000)]))->toSql(),
            notify($channel)->withPayload($payload)->toSql(),
        ));

        if ($sent === false) {
            throw new RuntimeException(sprintf(
                'Failed to send the scheduled notification: %s',
                pg_last_error($connection),
            ));
        }
    }
}
