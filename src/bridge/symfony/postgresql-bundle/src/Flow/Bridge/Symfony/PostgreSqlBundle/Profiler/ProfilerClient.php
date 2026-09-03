<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Profiler;

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
use Throwable;

use function count;
use function debug_backtrace;
use function hrtime;
use function is_array;
use function is_int;
use function is_string;
use function str_starts_with;

use const DEBUG_BACKTRACE_IGNORE_ARGS;

final class ProfilerClient implements Client
{
    private const array INSTRUMENTATION_NAMESPACES = ['Flow\\PostgreSql\\', __NAMESPACE__ . '\\'];

    public function __construct(
        private readonly Client $client,
        private readonly QueryRecorder $recorder,
        private readonly string $connection = 'default',
    ) {}

    public function beginTransaction(): void
    {
        $this->client->beginTransaction();
    }

    public function close(): void
    {
        $this->client->close();
    }

    public function commit(): void
    {
        $this->client->commit();
    }

    public function converters(): ValueConverters
    {
        return $this->client->converters();
    }

    public function cursor(Sql|string $sql, array $parameters = []): Cursor
    {
        $statement = $sql instanceof Sql ? $sql->toSql() : $sql;
        $start = hrtime(true);

        try {
            $cursor = $this->client->cursor($sql, $parameters);
        } catch (Throwable $e) {
            $this->recorder->add($this->failure($statement, $parameters, $start, $e));

            throw $e;
        }

        // Cursors are lazy; record the statement without consuming rows.
        $this->recorder->add(
            new RecordedQuery(
                $statement,
                $parameters,
                $this->elapsedMs($start),
                null,
                false,
                null,
                $this->connection,
                $this->callerLocation(),
            ),
        );

        return $cursor;
    }

    /**
     * @return list<array{name: string, type: ColumnType}>
     */
    public function describe(Sql|string $sql, array $parameters = []): array
    {
        // A zero-row probe has no row count to record, exactly as explain() has none.
        return $this->record($sql, $parameters, fn(): array => $this->client->describe($sql, $parameters), null);
    }

    public function execute(Sql|string $sql, array $parameters = []): int
    {
        return $this->record(
            $sql,
            $parameters,
            fn(): int => $this->client->execute($sql, $parameters),
            static fn(int $affected): int => $affected,
        );
    }

    public function explain(Sql|string $sql, array $parameters = [], ?ExplainConfig $config = null): Plan
    {
        return $this->record($sql, $parameters, fn(): Plan => $this->client->explain($sql, $parameters, $config), null);
    }

    public function fetch(Sql|string $sql, array $parameters = []): ?array
    {
        return $this->record(
            $sql,
            $parameters,
            fn(): ?array => $this->client->fetch($sql, $parameters),
            static fn(?array $row): int => $row !== null ? 1 : 0,
        );
    }

    public function fetchAll(Sql|string $sql, array $parameters = []): array
    {
        return $this->record(
            $sql,
            $parameters,
            fn(): array => $this->client->fetchAll($sql, $parameters),
            static fn(array $rows): int => count($rows),
        );
    }

    public function fetchAllInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): array
    {
        return $this->record(
            $sql,
            $parameters,
            fn(): array => $this->client->fetchAllInto($mapper, $sql, $parameters),
            static fn(array $rows): int => count($rows),
        );
    }

    public function fetchInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        return $this->record(
            $sql,
            $parameters,
            fn(): mixed => $this->client->fetchInto($mapper, $sql, $parameters),
            static fn(mixed $result): int => $result !== null ? 1 : 0,
        );
    }

    public function fetchOne(Sql|string $sql, array $parameters = []): ?array
    {
        return $this->record(
            $sql,
            $parameters,
            fn(): ?array => $this->client->fetchOne($sql, $parameters),
            static fn(?array $row): int => $row !== null ? 1 : 0,
        );
    }

    public function fetchOneInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        return $this->record(
            $sql,
            $parameters,
            fn(): mixed => $this->client->fetchOneInto($mapper, $sql, $parameters),
            static fn(mixed $result): int => $result !== null ? 1 : 0,
        );
    }

    public function fetchScalar(Sql|string $sql, array $parameters = []): mixed
    {
        return $this->record(
            $sql,
            $parameters,
            fn(): mixed => $this->client->fetchScalar($sql, $parameters),
            static fn(mixed $value): int => $value !== null ? 1 : 0,
        );
    }

    public function fetchScalarBool(Sql|string $sql, array $parameters = []): bool
    {
        return $this->record(
            $sql,
            $parameters,
            fn(): bool => $this->client->fetchScalarBool($sql, $parameters),
            static fn(bool $value): int => 1,
        );
    }

    public function fetchScalarFloat(Sql|string $sql, array $parameters = []): float
    {
        return $this->record(
            $sql,
            $parameters,
            fn(): float => $this->client->fetchScalarFloat($sql, $parameters),
            static fn(float $value): int => 1,
        );
    }

    public function fetchScalarInt(Sql|string $sql, array $parameters = []): int
    {
        return $this->record(
            $sql,
            $parameters,
            fn(): int => $this->client->fetchScalarInt($sql, $parameters),
            static fn(int $value): int => 1,
        );
    }

    public function fetchScalarString(Sql|string $sql, array $parameters = []): string
    {
        return $this->record(
            $sql,
            $parameters,
            fn(): string => $this->client->fetchScalarString($sql, $parameters),
            static fn(string $value): int => 1,
        );
    }

    public function fetchSingle(Sql|string $sql, array $parameters = []): array
    {
        return $this->record(
            $sql,
            $parameters,
            fn(): array => $this->client->fetchSingle($sql, $parameters),
            static fn(array $row): int => 1,
        );
    }

    public function fetchSingleInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        return $this->record(
            $sql,
            $parameters,
            fn(): mixed => $this->client->fetchSingleInto($mapper, $sql, $parameters),
            static fn(mixed $result): int => $result !== null ? 1 : 0,
        );
    }

    public function getTransactionNestingLevel(): int
    {
        return $this->client->getTransactionNestingLevel();
    }

    public function isAutoCommit(): bool
    {
        return $this->client->isAutoCommit();
    }

    public function isConnected(): bool
    {
        return $this->client->isConnected();
    }

    public function lastInsertId(string $sequenceName): int|string
    {
        return $this->client->lastInsertId($sequenceName);
    }

    public function listen(string $channel): void
    {
        $this->client->listen($channel);
    }

    public function parameters(): ConnectionParameters
    {
        return $this->client->parameters();
    }

    public function rollBack(): void
    {
        $this->client->rollBack();
    }

    public function setAutoCommit(bool $autoCommit): void
    {
        $this->client->setAutoCommit($autoCommit);
    }

    public function transaction(callable $callback): mixed
    {
        return $this->client->transaction($callback);
    }

    public function unlisten(string $channel): void
    {
        $this->client->unlisten($channel);
    }

    public function wait(int $milliseconds): ?Notification
    {
        return $this->client->wait($milliseconds);
    }

    /**
     * @template T
     *
     * @param list<mixed> $parameters
     * @param callable(): T $operation
     * @param null|callable(T): int $rowCountExtractor
     *
     * @return T
     */
    private function record(
        Sql|string $sql,
        array $parameters,
        callable $operation,
        ?callable $rowCountExtractor,
    ): mixed {
        $statement = $sql instanceof Sql ? $sql->toSql() : $sql;
        $start = hrtime(true);

        try {
            $result = $operation();
        } catch (Throwable $e) {
            $this->recorder->add($this->failure($statement, $parameters, $start, $e));

            throw $e;
        }

        $this->recorder->add(
            new RecordedQuery(
                $statement,
                $parameters,
                $this->elapsedMs($start),
                $rowCountExtractor === null ? null : $rowCountExtractor($result),
                false,
                null,
                $this->connection,
                $this->callerLocation(),
            ),
        );

        return $result;
    }

    /**
     * @param list<mixed> $parameters
     */
    private function failure(string $statement, array $parameters, int|float $start, Throwable $e): RecordedQuery
    {
        return new RecordedQuery(
            $statement,
            $parameters,
            $this->elapsedMs($start),
            null,
            true,
            $e->getMessage(),
            $this->connection,
            $this->callerLocation(),
        );
    }

    private function elapsedMs(int|float $start): float
    {
        return (hrtime(true) - $start) / 1_000_000;
    }

    /**
     * Application code location that issued the query, for the Web Profiler "caller" column.
     *
     * A backtrace frame's file/line is the *call site* (where the frame's function was called from),
     * while class/function is the callee. So the application caller is the file/line of the
     * shallowest instrumentation frame — the last frame belonging to this decorator or to the
     * PostgreSQL client itself before control crosses into application code.
     */
    private function callerLocation(): ?string
    {
        $candidate = null;

        // @mago-expect analysis:mixed-assignment
        foreach (debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 20) as $frame) {
            if (!is_array($frame)) {
                continue;
            }

            // @mago-expect analysis:mixed-assignment
            $class = $frame['class'] ?? null;

            if (!is_string($class)) {
                return $candidate;
            }

            $instrumentation = false;

            foreach (self::INSTRUMENTATION_NAMESPACES as $namespace) {
                if (str_starts_with($class, $namespace)) {
                    $instrumentation = true;

                    break;
                }
            }

            if (!$instrumentation) {
                return $candidate;
            }

            // @mago-expect analysis:mixed-assignment
            $file = $frame['file'] ?? null;

            if (is_string($file)) {
                // @mago-expect analysis:mixed-assignment
                $line = $frame['line'] ?? 0;
                $candidate = $file . ':' . (is_int($line) ? $line : 0);
            }
        }

        return $candidate;
    }
}
