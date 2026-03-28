<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration;

use function Flow\PostgreSql\DSL\{drop, pgsql_client, pgsql_connection_dsn};

use Flow\PostgreSql\Client\Client;

final class PostgreSqlContext
{
    public readonly Client $client;

    public function __construct()
    {
        $dsn = \getenv('PGSQL_DATABASE_URL');

        if (!$dsn) {
            throw new \RuntimeException('PGSQL_DATABASE_URL environment variable is not set');
        }

        $this->client = pgsql_client(pgsql_connection_dsn($dsn));
    }

    public function client() : Client
    {
        return $this->client;
    }

    public function close() : void
    {
        $this->client->close();
    }

    public function dropDomainIfExists(string $domain) : void
    {
        $this->client->execute(drop()->domain($domain)->ifExists()->cascade()->toSql());
    }

    public function dropFunctionIfExists(string $function) : void
    {
        $this->client->execute(drop()->function($function)->ifExists()->cascade()->toSql());
    }

    public function dropIndexIfExists(string $index) : void
    {
        $this->client->execute(drop()->index($index)->ifExists()->cascade()->toSql());
    }

    public function dropMaterializedViewIfExists(string $view) : void
    {
        $this->client->execute(drop()->materializedView($view)->ifExists()->cascade()->toSql());
    }

    public function dropSchemaIfExists(string $schema) : void
    {
        $this->client->execute(drop()->schema($schema)->ifExists()->cascade()->toSql());
    }

    public function dropSequenceIfExists(string $sequence) : void
    {
        $this->client->execute(drop()->sequence($sequence)->ifExists()->cascade()->toSql());
    }

    public function dropTableIfExists(string $table) : void
    {
        $this->client->execute(drop()->table($table)->ifExists()->cascade()->toSql());
    }

    public function dropTriggerIfExists(string $trigger, string $table) : void
    {
        $this->client->execute(drop()->trigger($trigger)->on($table)->ifExists()->cascade()->toSql());
    }

    public function dropTypeIfExists(string $type) : void
    {
        $this->client->execute(drop()->type($type)->ifExists()->cascade()->toSql());
    }

    public function dropViewIfExists(string $view) : void
    {
        $this->client->execute(drop()->view($view)->ifExists()->cascade()->toSql());
    }
}
