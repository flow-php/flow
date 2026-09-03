<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Double;

use Flow\PostgreSql\AST\Transformers\ExplainConfig;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\ConnectionParameters;
use Flow\PostgreSql\Client\Cursor;
use Flow\PostgreSql\Client\Notification;
use Flow\PostgreSql\Client\RowMapper;
use Flow\PostgreSql\Client\Types\ValueConverters;
use Flow\PostgreSql\Explain\Plan\Plan;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Sql;
use RuntimeException;

class CacheSpyClient implements Client
{
    /**
     * @var list<array{sql: string, parameters: array<int, mixed>}>
     */
    public array $executedQueries = [];

    public function beginTransaction(): void {}

    public function close(): void
    {
        throw new RuntimeException('Not implemented');
    }

    public function commit(): void {}

    public function converters(): ValueConverters
    {
        throw new RuntimeException('Not implemented');
    }

    public function cursor(Sql|string $sql, array $parameters = []): Cursor
    {
        throw new RuntimeException('Not implemented');
    }

    /**
     * @return list<array{name: string, type: ColumnType}>
     */
    public function describe(Sql|string $sql, array $parameters = []): array
    {
        throw new RuntimeException('Not implemented');
    }

    public function execute(Sql|string $sql, array $parameters = []): int
    {
        $this->executedQueries[] = ['sql' => $sql instanceof Sql ? $sql->toSql() : $sql, 'parameters' => $parameters];

        return 0;
    }

    public function explain(Sql|string $sql, array $parameters = [], ?ExplainConfig $config = null): Plan
    {
        throw new RuntimeException('Not implemented');
    }

    public function fetch(Sql|string $sql, array $parameters = []): ?array
    {
        $this->executedQueries[] = ['sql' => $sql instanceof Sql ? $sql->toSql() : $sql, 'parameters' => $parameters];

        return null;
    }

    public function fetchAll(Sql|string $sql, array $parameters = []): array
    {
        $this->executedQueries[] = ['sql' => $sql instanceof Sql ? $sql->toSql() : $sql, 'parameters' => $parameters];

        return [];
    }

    public function fetchAllInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): array
    {
        throw new RuntimeException('Not implemented');
    }

    public function fetchInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        throw new RuntimeException('Not implemented');
    }

    public function fetchOne(Sql|string $sql, array $parameters = []): ?array
    {
        throw new RuntimeException('Not implemented');
    }

    public function fetchOneInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        throw new RuntimeException('Not implemented');
    }

    public function fetchScalar(Sql|string $sql, array $parameters = []): mixed
    {
        throw new RuntimeException('Not implemented');
    }

    public function fetchScalarBool(Sql|string $sql, array $parameters = []): bool
    {
        throw new RuntimeException('Not implemented');
    }

    public function fetchScalarFloat(Sql|string $sql, array $parameters = []): float
    {
        throw new RuntimeException('Not implemented');
    }

    public function fetchScalarInt(Sql|string $sql, array $parameters = []): int
    {
        throw new RuntimeException('Not implemented');
    }

    public function fetchScalarString(Sql|string $sql, array $parameters = []): string
    {
        throw new RuntimeException('Not implemented');
    }

    public function fetchSingle(Sql|string $sql, array $parameters = []): array
    {
        throw new RuntimeException('Not implemented');
    }

    public function fetchSingleInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        throw new RuntimeException('Not implemented');
    }

    public function getTransactionNestingLevel(): int
    {
        return 0;
    }

    public function isAutoCommit(): bool
    {
        throw new RuntimeException('Not implemented');
    }

    public function isConnected(): bool
    {
        throw new RuntimeException('Not implemented');
    }

    public function lastInsertId(string $sequenceName): int|string
    {
        throw new RuntimeException('Not implemented');
    }

    public function listen(string $channel): void
    {
        throw new RuntimeException('Not implemented');
    }

    public function parameters(): ConnectionParameters
    {
        throw new RuntimeException('Not implemented');
    }

    public function rollBack(): void {}

    public function setAutoCommit(bool $autoCommit): void
    {
        throw new RuntimeException('Not implemented');
    }

    public function transaction(callable $callback): mixed
    {
        return $callback($this);
    }

    public function unlisten(string $channel): void
    {
        throw new RuntimeException('Not implemented');
    }

    public function wait(int $milliseconds): ?Notification
    {
        throw new RuntimeException('Not implemented');
    }
}
