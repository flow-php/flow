<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Mother;

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

/**
 * Inert Client used by Context-related ObjectMothers. Every method throws —
 * tests that exercise the Client must use a different double.
 */
final class StubClient implements Client
{
    public function beginTransaction(): void
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function close(): void
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function commit(): void
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function converters(): ValueConverters
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function cursor(Sql|string $sql, array $parameters = []): Cursor
    {
        throw new RuntimeException('StubClient is inert');
    }

    /**
     * @return list<array{name: string, type: ColumnType}>
     */
    public function describe(Sql|string $sql, array $parameters = []): array
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function execute(Sql|string $sql, array $parameters = []): int
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function explain(Sql|string $sql, array $parameters = [], ?ExplainConfig $config = null): Plan
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function fetch(Sql|string $sql, array $parameters = []): ?array
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function fetchAll(Sql|string $sql, array $parameters = []): array
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function fetchAllInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): array
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function fetchInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function fetchOne(Sql|string $sql, array $parameters = []): ?array
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function fetchOneInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function fetchScalar(Sql|string $sql, array $parameters = []): mixed
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function fetchScalarBool(Sql|string $sql, array $parameters = []): bool
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function fetchScalarFloat(Sql|string $sql, array $parameters = []): float
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function fetchScalarInt(Sql|string $sql, array $parameters = []): int
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function fetchScalarString(Sql|string $sql, array $parameters = []): string
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function fetchSingle(Sql|string $sql, array $parameters = []): array
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function fetchSingleInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function getTransactionNestingLevel(): int
    {
        return 0;
    }

    public function isAutoCommit(): bool
    {
        return true;
    }

    public function isConnected(): bool
    {
        return false;
    }

    public function lastInsertId(string $sequenceName): int|string
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function listen(string $channel): void
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function parameters(): ConnectionParameters
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function rollBack(): void
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function setAutoCommit(bool $autoCommit): void
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function transaction(callable $callback): mixed
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function unlisten(string $channel): void
    {
        throw new RuntimeException('StubClient is inert');
    }

    public function wait(int $milliseconds): ?Notification
    {
        return null;
    }
}
