<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLSession\Tests\Context;

use Flow\Bridge\Symfony\PostgreSQLSession\FlowPostgreSqlSessionHandler;
use Flow\Bridge\Symfony\PostgreSQLSession\Tests\Unit\Double\SpyClient;

use function array_slice;
use function count;
use function str_starts_with;

final class PostgreSqlSessionContext
{
    public SpyClient $client;

    public function __construct()
    {
        $this->client = new SpyClient();
    }

    /**
     * @param array{db_table?: string, db_schema?: string, db_id_col?: string, db_data_col?: string, db_lifetime_col?: string, db_time_col?: string, lock_mode?: int, ttl?: null|int} $options
     */
    public function handler(array $options = []): FlowPostgreSqlSessionHandler
    {
        $handler = new FlowPostgreSqlSessionHandler($this->client, $options);
        $handler->open('', 'test_session');

        return $handler;
    }

    /**
     * Run the given block, then return only queries whose SQL starts with the given keyword.
     *
     * @param non-empty-string $keyword
     *
     * @return list<array{sql: string, parameters: array<int, mixed>}>
     */
    public function onlyQueries(string $keyword, callable $action): array
    {
        $before = count($this->client->executedQueries);
        $action();

        $matching = [];

        foreach (array_slice($this->client->executedQueries, $before) as $query) {
            if (str_starts_with($query['sql'], $keyword)) {
                $matching[] = $query;
            }
        }

        return $matching;
    }
}
