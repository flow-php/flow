<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Double;

use Flow\PostgreSql\AST\Transformers\ExplainConfig;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\ConnectionParameters;
use Flow\PostgreSql\Client\ConvertedParameters;
use Flow\PostgreSql\Client\Cursor;
use Flow\PostgreSql\Client\Exception\QueryException;
use Flow\PostgreSql\Client\Notification;
use Flow\PostgreSql\Client\RowMapper;
use Flow\PostgreSql\Client\Types\ValueConverters;
use Flow\PostgreSql\Explain\Plan\Cost;
use Flow\PostgreSql\Explain\Plan\Plan;
use Flow\PostgreSql\Explain\Plan\PlanNode;
use Flow\PostgreSql\Explain\Plan\PlanNodeType;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Sql;
use RuntimeException;

use function Flow\PostgreSql\DSL\pgsql_connection_params;

/**
 * Configurable {@see Client} test double for exercising decorators. Query methods return canned
 * values (or throw a {@see QueryException} when armed via {@see self::failNextQuery()}); transaction
 * and connection methods record that they were called.
 */
final class FakeClient implements Client
{
    /** @var list<string> */
    public array $delegated = [];

    /** @var list<array{name: string, type: ColumnType}> */
    public array $describeReturn = [];

    public int $executeReturn = 1;

    /** @var null|array<string, mixed> */
    public ?array $fetchReturn = null;

    /** @var array<int, array<string, mixed>> */
    public array $fetchAllReturn = [];

    public int $fetchScalarIntReturn = 0;

    private ?QueryException $failure = null;

    public function __construct(
        private readonly ?Cursor $cursor = null,
    ) {}

    public function failNextQuery(QueryException $exception): void
    {
        $this->failure = $exception;
    }

    public function beginTransaction(): void
    {
        $this->delegated[] = 'beginTransaction';
    }

    public function close(): void
    {
        $this->delegated[] = 'close';
    }

    public function commit(): void
    {
        $this->delegated[] = 'commit';
    }

    public function converters(): ValueConverters
    {
        $this->delegated[] = 'converters';

        return ValueConverters::create();
    }

    public function cursor(Sql|string $sql, array $parameters = []): Cursor
    {
        $this->guard();

        if ($this->cursor === null) {
            throw new RuntimeException('no cursor configured');
        }

        return $this->cursor;
    }

    /**
     * @return list<array{name: string, type: ColumnType}>
     */
    public function describe(Sql|string $sql, array $parameters = []): array
    {
        $this->guard();

        return $this->describeReturn;
    }

    public function execute(Sql|string $sql, array|ConvertedParameters $parameters = []): int
    {
        $this->guard();

        return $this->executeReturn;
    }

    public function explain(Sql|string $sql, array $parameters = [], ?ExplainConfig $config = null): Plan
    {
        $this->guard();

        return new Plan(new PlanNode(PlanNodeType::SEQ_SCAN, new Cost(0.0, 10.0), 100, 8));
    }

    public function fetch(Sql|string $sql, array $parameters = []): ?array
    {
        $this->guard();

        return $this->fetchReturn;
    }

    public function fetchAll(Sql|string $sql, array $parameters = []): array
    {
        $this->guard();

        return $this->fetchAllReturn;
    }

    public function fetchAllInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): array
    {
        $this->guard();

        return [];
    }

    public function fetchInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        $this->guard();

        return null;
    }

    public function fetchOne(Sql|string $sql, array $parameters = []): ?array
    {
        $this->guard();

        return $this->fetchReturn;
    }

    public function fetchOneInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        $this->guard();

        return null;
    }

    public function fetchScalar(Sql|string $sql, array $parameters = []): mixed
    {
        $this->guard();

        return $this->fetchScalarIntReturn;
    }

    public function fetchScalarBool(Sql|string $sql, array $parameters = []): bool
    {
        $this->guard();

        return true;
    }

    public function fetchScalarFloat(Sql|string $sql, array $parameters = []): float
    {
        $this->guard();

        return 0.0;
    }

    public function fetchScalarInt(Sql|string $sql, array $parameters = []): int
    {
        $this->guard();

        return $this->fetchScalarIntReturn;
    }

    public function fetchScalarString(Sql|string $sql, array $parameters = []): string
    {
        $this->guard();

        return '';
    }

    public function fetchSingle(Sql|string $sql, array $parameters = []): array
    {
        $this->guard();

        return $this->fetchReturn ?? [];
    }

    public function fetchSingleInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        $this->guard();

        return null;
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
        $this->delegated[] = 'isConnected';

        return true;
    }

    public function lastInsertId(string $sequenceName): int|string
    {
        return 0;
    }

    public function listen(string $channel): void
    {
        $this->delegated[] = 'listen';
    }

    public function parameters(): ConnectionParameters
    {
        $this->delegated[] = 'parameters';

        return pgsql_connection_params('testdb', 'localhost', 5432, 'user');
    }

    public function rollBack(): void
    {
        $this->delegated[] = 'rollBack';
    }

    public function setAutoCommit(bool $autoCommit): void
    {
        $this->delegated[] = 'setAutoCommit';
    }

    public function transaction(callable $callback): mixed
    {
        $this->delegated[] = 'transaction';

        return $callback($this);
    }

    public function unlisten(string $channel): void
    {
        $this->delegated[] = 'unlisten';
    }

    public function wait(int $milliseconds): ?Notification
    {
        return null;
    }

    private function guard(): void
    {
        if ($this->failure !== null) {
            $failure = $this->failure;
            $this->failure = null;

            throw $failure;
        }
    }
}
