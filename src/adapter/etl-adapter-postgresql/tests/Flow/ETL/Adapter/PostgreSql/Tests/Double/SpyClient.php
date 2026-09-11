<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Double;

use Flow\PostgreSql\AST\Transformers\ExplainConfig;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\ConnectionParameters;
use Flow\PostgreSql\Client\ConvertedParameters;
use Flow\PostgreSql\Client\Cursor;
use Flow\PostgreSql\Client\Exception\QueryException;
use Flow\PostgreSql\Client\Notification;
use Flow\PostgreSql\Client\RowMapper;
use Flow\PostgreSql\Client\Types\ValueConverters;
use Flow\PostgreSql\Explain\Plan\Plan;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Sql;
use RuntimeException;
use Throwable;

use function array_filter;
use function array_key_exists;
use function array_shift;
use function array_values;
use function count;

/**
 * Answers from what the test seeds, records every call it receives, and refuses everything the extractors and
 * the loader never reach.
 */
final class SpyClient implements Client
{
    /** @var list<string> */
    public array $calls = [];

    /** @var list<array{name: string, type: ColumnType}> */
    private array $describeAnswer = [];

    private ?Throwable $describeFailure = null;

    /** @var list<Cursor> */
    private array $cursors = [];

    /** @var list<list<mixed>> */
    public array $describeParameters = [];

    private int $scalarIntAnswer = 0;

    /** @var array<int, Throwable> */
    private array $executeFailures = [];

    public function __construct(
        private int $transactionNestingLevel = 0,
    ) {}

    public function callsTo(string $method): int
    {
        return count(array_filter($this->calls, static fn(string $call): bool => $call === $method));
    }

    /**
     * The $call-th execute(), counting from 1, throws $failure.
     */
    public function willFailExecute(int $call, Throwable $failure): self
    {
        $this->executeFailures[$call] = $failure;

        return $this;
    }

    public function willCountTotal(int $total): self
    {
        $this->scalarIntAnswer = $total;

        return $this;
    }

    /**
     * @param list<array{name: string, type: ColumnType}> $columns
     */
    public function willDescribe(array $columns): self
    {
        $this->describeAnswer = $columns;

        return $this;
    }

    public function willThrowFromDescribe(Throwable $failure): self
    {
        $this->describeFailure = $failure;

        return $this;
    }

    public function willRefuseDescribe(QueryException $failure): self
    {
        $this->describeFailure = $failure;

        return $this;
    }

    public function willReturnCursors(Cursor ...$cursors): self
    {
        $this->cursors = array_values($cursors);

        return $this;
    }

    public function beginTransaction(): void
    {
        $this->calls[] = 'beginTransaction';
        $this->transactionNestingLevel++;
    }

    public function commit(): void
    {
        $this->calls[] = 'commit';
        $this->transactionNestingLevel--;
    }

    public function cursor(Sql|string $sql, array $parameters = []): Cursor
    {
        $this->calls[] = 'cursor';

        if ($this->cursors === []) {
            throw new RuntimeException('SpyClient has no cursor left to hand out');
        }

        return array_shift($this->cursors);
    }

    /**
     * @return list<array{name: string, type: ColumnType}>
     */
    public function describe(Sql|string $sql, array $parameters = []): array
    {
        $this->calls[] = 'describe';
        $this->describeParameters[] = $parameters;

        if ($this->describeFailure !== null) {
            throw $this->describeFailure;
        }

        return $this->describeAnswer;
    }

    public function getTransactionNestingLevel(): int
    {
        return $this->transactionNestingLevel;
    }

    public function rollBack(): void
    {
        $this->calls[] = 'rollBack';
        $this->transactionNestingLevel--;
    }

    public function close(): void
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function converters(): ValueConverters
    {
        return new ValueConverters();
    }

    public function execute(Sql|string $sql, array|ConvertedParameters $parameters = []): int
    {
        $this->calls[] = 'execute';

        $execution = $this->callsTo('execute');

        if (array_key_exists($execution, $this->executeFailures)) {
            throw $this->executeFailures[$execution];
        }

        return 0;
    }

    public function explain(Sql|string $sql, array $parameters = [], ?ExplainConfig $config = null): Plan
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function fetch(Sql|string $sql, array $parameters = []): ?array
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function fetchAll(Sql|string $sql, array $parameters = []): array
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function fetchAllInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): array
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function fetchInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function fetchOne(Sql|string $sql, array $parameters = []): ?array
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function fetchOneInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function fetchScalar(Sql|string $sql, array $parameters = []): mixed
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function fetchScalarBool(Sql|string $sql, array $parameters = []): bool
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function fetchScalarFloat(Sql|string $sql, array $parameters = []): float
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function fetchScalarInt(Sql|string $sql, array $parameters = []): int
    {
        $this->calls[] = 'fetchScalarInt';

        return $this->scalarIntAnswer;
    }

    public function fetchScalarString(Sql|string $sql, array $parameters = []): string
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function fetchSingle(Sql|string $sql, array $parameters = []): array
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function fetchSingleInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function isAutoCommit(): bool
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function isConnected(): bool
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function lastInsertId(string $sequenceName): string|int
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function listen(string $channel): void
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function parameters(): ConnectionParameters
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function setAutoCommit(bool $autoCommit): void
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function transaction(callable $callback): mixed
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function unlisten(string $channel): void
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }

    public function wait(int $milliseconds): ?Notification
    {
        throw new RuntimeException('SpyClient does not implement ' . __FUNCTION__);
    }
}
