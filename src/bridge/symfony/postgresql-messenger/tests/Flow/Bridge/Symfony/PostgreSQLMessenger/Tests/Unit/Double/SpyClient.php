<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Unit\Double;

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
use Throwable;

class SpyClient implements Client
{
    /**
     * @var list<array{sql: string, parameters: array<int, mixed>}>
     */
    public array $executedQueries = [];

    public int $executeReturn = 0;

    /**
     * @var list<array<string, mixed>>
     */
    public array $fetchAllReturn = [];

    /**
     * @var null|array<string, mixed>
     */
    public ?array $fetchReturn = null;

    public int $fetchScalarIntReturn = 0;

    /**
     * @var array<string, mixed>
     */
    public array $fetchSingleReturn = [];

    public int $transactionCallCount = 0;

    public int $transactionLevel = 0;

    public function beginTransaction(): void
    {
        $this->transactionLevel++;
    }

    public function close(): void
    {
        throw new RuntimeException('Not implemented');
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
        $this->executedQueries[] = [
            'sql' => $sql instanceof Sql ? $sql->toSql() : $sql,
            'parameters' => $parameters instanceof ConvertedParameters ? $parameters->values : $parameters,
        ];

        return $this->executeReturn;
    }

    public function explain(Sql|string $sql, array $parameters = [], ?ExplainConfig $config = null): Plan
    {
        throw new RuntimeException('Not implemented');
    }

    public function fetch(Sql|string $sql, array $parameters = []): ?array
    {
        $this->executedQueries[] = ['sql' => $sql instanceof Sql ? $sql->toSql() : $sql, 'parameters' => $parameters];

        return $this->fetchReturn;
    }

    public function fetchAll(Sql|string $sql, array $parameters = []): array
    {
        $this->executedQueries[] = ['sql' => $sql instanceof Sql ? $sql->toSql() : $sql, 'parameters' => $parameters];

        return $this->fetchAllReturn;
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
        $this->executedQueries[] = ['sql' => $sql instanceof Sql ? $sql->toSql() : $sql, 'parameters' => $parameters];

        return $this->fetchScalarIntReturn;
    }

    public function fetchScalarString(Sql|string $sql, array $parameters = []): string
    {
        throw new RuntimeException('Not implemented');
    }

    public function fetchSingle(Sql|string $sql, array $parameters = []): array
    {
        $this->executedQueries[] = ['sql' => $sql instanceof Sql ? $sql->toSql() : $sql, 'parameters' => $parameters];

        return $this->fetchSingleReturn;
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
        $this->transactionCallCount++;
        $this->beginTransaction();

        try {
            $result = $callback($this);
            $this->commit();

            return $result;
        } catch (Throwable $e) {
            $this->rollBack();

            throw $e;
        }
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
