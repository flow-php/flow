<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Double;

use Flow\ETL\Adapter\PostgreSql\Pagination\Order;
use Flow\ETL\Adapter\PostgreSql\Tests\Mother\ColumnMother;
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

use function array_fill_keys;
use function array_filter;
use function array_key_exists;
use function array_slice;
use function array_values;

/**
 * Serves rows already sorted by one key the way PostgreSQL pages them: a cursor value bounds the page strictly, NULL
 * keys sort into the first page only, and the NULL check finds them.
 */
final class SortedRowsClient implements Client
{
    public int $cursorCalls = 0;

    /**
     * @param list<string> $columns
     * @param list<array<string, null|int|string>> $rows
     */
    public function __construct(
        private readonly array $columns,
        private readonly array $rows,
        private readonly string $key,
        private readonly Order $order,
    ) {}

    public function cursor(Sql|string $sql, array $parameters = []): Cursor
    {
        $this->cursorCalls++;
        $limit = (int) $parameters[0];

        if (!array_key_exists(1, $parameters)) {
            return new StubCursor(array_slice($this->rows, 0, $limit));
        }

        $after = (int) $parameters[1];

        return new StubCursor(array_slice(
            array_values(array_filter(
                $this->rows,
                fn(array $row): bool => (
                    $row[$this->key] !== null
                    && ($this->order === Order::ASC ? $row[$this->key] > $after : $row[$this->key] < $after)
                ),
            )),
            0,
            $limit,
        ));
    }

    /**
     * @return list<array{name: string, type: ColumnType}>
     */
    public function describe(Sql|string $sql, array $parameters = []): array
    {
        return ColumnMother::of(array_fill_keys($this->columns, 'int8'));
    }

    public function fetchOne(Sql|string $sql, array $parameters = []): ?array
    {
        foreach ($this->rows as $row) {
            if ($row[$this->key] === null) {
                return ['?column?' => 1];
            }
        }

        return null;
    }

    public function beginTransaction(): void
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function commit(): void
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function getTransactionNestingLevel(): int
    {
        return 0;
    }

    public function rollBack(): void
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function close(): void
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function converters(): ValueConverters
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function execute(Sql|string $sql, array|ConvertedParameters $parameters = []): int
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function explain(Sql|string $sql, array $parameters = [], ?ExplainConfig $config = null): Plan
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function fetch(Sql|string $sql, array $parameters = []): ?array
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function fetchAll(Sql|string $sql, array $parameters = []): array
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function fetchAllInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): array
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function fetchInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function fetchOneInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function fetchScalar(Sql|string $sql, array $parameters = []): mixed
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function fetchScalarBool(Sql|string $sql, array $parameters = []): bool
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function fetchScalarFloat(Sql|string $sql, array $parameters = []): float
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function fetchScalarInt(Sql|string $sql, array $parameters = []): int
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function fetchScalarString(Sql|string $sql, array $parameters = []): string
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function fetchSingle(Sql|string $sql, array $parameters = []): array
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function fetchSingleInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function isAutoCommit(): bool
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function isConnected(): bool
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function lastInsertId(string $sequenceName): string|int
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function listen(string $channel): void
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function parameters(): ConnectionParameters
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function setAutoCommit(bool $autoCommit): void
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function transaction(callable $callback): mixed
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function unlisten(string $channel): void
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }

    public function wait(int $milliseconds): ?Notification
    {
        throw new RuntimeException('SortedRowsClient does not implement ' . __FUNCTION__);
    }
}
