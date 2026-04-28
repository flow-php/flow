<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLCache\Tests\Context;

use Flow\Bridge\Symfony\PostgreSQLCache\FlowPostgreSqlCacheAdapter;
use Flow\Bridge\Symfony\PostgreSQLCache\Tests\Unit\Double\{SpyClient, SpyMarshaller};
use Symfony\Component\Cache\Marshaller\MarshallerInterface;

final class PostgreSqlCacheContext
{
    public SpyClient $client;

    public function __construct()
    {
        $this->client = new SpyClient();
    }

    /**
     * @param array{db_table?: string, db_schema?: string, db_id_col?: string, db_data_col?: string, db_lifetime_col?: string, db_time_col?: string} $options
     */
    public function adapter(
        string $namespace = '',
        int $defaultLifetime = 0,
        array $options = [],
        ?MarshallerInterface $marshaller = null,
    ) : FlowPostgreSqlCacheAdapter {
        return new FlowPostgreSqlCacheAdapter($this->client, $namespace, $defaultLifetime, $options, $marshaller);
    }

    /**
     * Run the given block, then return only the INSERT queries captured by the spy.
     * Useful when the action under test reads the cache first (which produces a SELECT)
     * and we only care about the write side-effect.
     *
     * @return list<array{sql: string, parameters: array<int, mixed>}>
     */
    public function onlyInsertQueries(callable $action) : array
    {
        $before = \count($this->client->executedQueries);
        $action();

        $inserts = [];

        foreach (\array_slice($this->client->executedQueries, $before) as $query) {
            if (\str_starts_with($query['sql'], 'INSERT')) {
                $inserts[] = $query;
            }
        }

        return $inserts;
    }

    public function spyMarshaller(string ...$failKeys) : SpyMarshaller
    {
        return new SpyMarshaller(\array_values($failKeys));
    }
}
