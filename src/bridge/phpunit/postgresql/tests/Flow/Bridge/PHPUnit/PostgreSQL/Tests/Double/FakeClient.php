<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\PostgreSQL\Tests\Double;

use Flow\PostgreSql\AST\Transformers\ExplainConfig;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\ConnectionParameters;
use Flow\PostgreSql\Client\ConvertedParameters;
use Flow\PostgreSql\Client\Cursor;
use Flow\PostgreSql\Client\Notification;
use Flow\PostgreSql\Client\RowMapper;
use Flow\PostgreSql\Client\Types\ValueConverters;
use Flow\PostgreSql\Explain\Plan\Plan;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Sql;
use RuntimeException;

final class FakeClient implements Client
{
    public bool $connected = true;

    public int $transactionLevel = 0;

    public function beginTransaction(): void
    {
        $this->transactionLevel++;
    }

    public function close(): void
    {
        $this->connected = false;
    }

    public function commit(): void
    {
        $this->transactionLevel--;
    }

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

    public function execute(Sql|string $sql, array|ConvertedParameters $parameters = []): int
    {
        throw new RuntimeException('Not implemented');
    }

    public function explain(Sql|string $sql, array $parameters = [], ?ExplainConfig $config = null): Plan
    {
        throw new RuntimeException('Not implemented');
    }

    public function fetch(Sql|string $sql, array $parameters = []): ?array
    {
        throw new RuntimeException('Not implemented');
    }

    public function fetchAll(Sql|string $sql, array $parameters = []): array
    {
        throw new RuntimeException('Not implemented');
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
        return $this->transactionLevel;
    }

    public function isAutoCommit(): bool
    {
        throw new RuntimeException('Not implemented');
    }

    public function isConnected(): bool
    {
        return $this->connected;
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

    public function rollBack(): void
    {
        $this->transactionLevel--;
    }

    public function setAutoCommit(bool $autoCommit): void
    {
        throw new RuntimeException('Not implemented');
    }

    public function transaction(callable $callback): mixed
    {
        throw new RuntimeException('Not implemented');
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
