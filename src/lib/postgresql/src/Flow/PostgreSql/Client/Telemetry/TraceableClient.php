<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Telemetry;

use Closure;
use Flow\PostgreSql\AST\Transformers\ExplainConfig;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\ConnectionParameters;
use Flow\PostgreSql\Client\Cursor;
use Flow\PostgreSql\Client\Exception\QueryException;
use Flow\PostgreSql\Client\Notification;
use Flow\PostgreSql\Client\RowMapper;
use Flow\PostgreSql\Client\Types\ValueConverters;
use Flow\PostgreSql\Explain\Plan\Plan;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\Telemetry\Context\Scope;
use Flow\Telemetry\Logger\Logger;
use Flow\Telemetry\Meter\Instrument\Histogram;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\SemConvAttributes;
use Flow\Telemetry\SemConvMetrics;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\Tracer;
use Throwable;

use function array_keys;
use function array_merge;
use function count;
use function Flow\PostgreSql\DSL\listen;
use function Flow\PostgreSql\DSL\unlisten;
use function hrtime;
use function rsort;
use function strlen;
use function substr;

/**
 * Decorator that adds telemetry instrumentation to a PostgreSQL client.
 *
 * Wraps all query and transaction operations with spans, metrics, and logs
 * following OpenTelemetry semantic conventions for database operations.
 */
final class TraceableClient implements Client
{
    private ?Logger $logger = null;

    private ?Histogram $operationDuration = null;

    private readonly ParameterFormatter $parameterFormatter;

    private readonly QueryAttributesExtractor $queryAttributesExtractor;

    private ?Histogram $returnedRows = null;

    private ?Tracer $tracer = null;

    /**
     * Keyed by nesting level; span and scope are always set and unset together.
     *
     * @var array<int, array{span: Span, scope: Scope}>
     */
    private array $transactionSpans = [];

    public function __construct(
        private readonly Client $client,
        private readonly PostgreSqlTelemetryConfig $telemetryConfig,
    ) {
        $this->queryAttributesExtractor = new QueryAttributesExtractor();
        $this->parameterFormatter = new ParameterFormatter();

        if (
            $this->telemetryConfig->options->traceQueries
            || $this->telemetryConfig->options->transactionSpans !== TransactionSpanMode::OFF
        ) {
            $this->tracer = $telemetryConfig->telemetry->tracer(
                'flow_php_postgresql',
                PackageVersion::get('flow-php/postgresql'),
            );
        }

        if ($this->telemetryConfig->options->collectMetrics) {
            $meter = $telemetryConfig->telemetry->meter(
                'flow_php_postgresql',
                PackageVersion::get('flow-php/postgresql'),
            );
            $this->operationDuration = $meter->createHistogram(
                SemConvMetrics::DB_CLIENT_OPERATION_DURATION,
                's',
                'Duration of database client operations',
                [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0],
            );
            $this->returnedRows = $meter->createHistogram(
                SemConvMetrics::DB_CLIENT_RESPONSE_RETURNED_ROWS,
                '{row}',
                'Number of rows returned by database operations',
                [1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0],
            );
        }

        if ($this->telemetryConfig->options->logQueries) {
            $this->logger = $telemetryConfig->telemetry->logger(
                'flow_php_postgresql',
                PackageVersion::get('flow-php/postgresql'),
            );
        }
    }

    public function beginTransaction(): void
    {
        $startTime = hrtime(true);
        $nestingLevel = $this->client->getTransactionNestingLevel() + 1;

        try {
            if ($this->telemetryConfig->options->transactionSpans === TransactionSpanMode::PER_OPERATION) {
                $this->traceTransactionOperation('BEGIN', $nestingLevel, fn() => $this->client->beginTransaction());
            } else {
                $this->client->beginTransaction();

                $tracer = $this->tracer;

                if (
                    $this->telemetryConfig->options->transactionSpans === TransactionSpanMode::GROUPED
                    && $tracer !== null
                ) {
                    // activated: a grouped transaction span is a logical scope - every query and nested
                    // savepoint until commit/rollback belongs under it
                    $transactionSpan = $tracer->span(
                        $this->buildTransactionSpanName('BEGIN', $nestingLevel),
                        SpanKind::CLIENT,
                        $this->buildTransactionAttributes($nestingLevel),
                    );
                    $this->transactionSpans[$nestingLevel] = [
                        'span' => $transactionSpan,
                        'scope' => $tracer->activate($transactionSpan),
                    ];
                }
            }

            $this->recordDuration($startTime, $this->buildTransactionMetricAttributes($nestingLevel, 'begin'));
        } catch (Throwable $e) {
            $this->recordDuration($startTime, $this->buildTransactionMetricAttributes($nestingLevel, 'begin'));

            throw $e;
        }
    }

    public function close(): void
    {
        $levels = array_keys($this->transactionSpans);
        rsort($levels);

        foreach ($levels as $level) {
            $span = $this->transactionSpans[$level]['span'];
            $span->setStatus(SpanStatus::error(
                'Transaction was neither committed nor rolled back before the connection was closed',
            ));
            $this->transactionSpans[$level]['scope']->detach();
            $this->tracer?->complete($span);
            unset($this->transactionSpans[$level]);
        }

        $this->client->close();
    }

    public function commit(): void
    {
        $startTime = hrtime(true);
        $nestingLevel = $this->client->getTransactionNestingLevel();

        try {
            if ($this->telemetryConfig->options->transactionSpans === TransactionSpanMode::PER_OPERATION) {
                $this->traceTransactionOperation('COMMIT', $nestingLevel, fn() => $this->client->commit());
            } else {
                $this->client->commit();
                $this->completeTransactionSpan($nestingLevel);
            }

            $this->recordDuration($startTime, $this->buildTransactionMetricAttributes($nestingLevel, 'commit'));
        } catch (Throwable $e) {
            $this->completeTransactionSpan($nestingLevel, $e);
            $this->recordDuration($startTime, $this->buildTransactionMetricAttributes($nestingLevel, 'commit'));

            throw $e;
        }
    }

    public function converters(): ValueConverters
    {
        return $this->client->converters();
    }

    public function cursor(Sql|string $sql, array $parameters = []): Cursor
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;
        $cursor = $this->client->cursor($sql, $parameters);

        if (!$this->telemetryConfig->options->traceQueries && !$this->telemetryConfig->options->collectMetrics) {
            return $cursor;
        }

        $this->logQuery($query, $parameters);

        return new TraceableCursor($cursor, $this->telemetryConfig, $this->client->parameters(), $query, $parameters);
    }

    /**
     * @return list<array{name: string, type: ColumnType}>
     */
    public function describe(Sql|string $sql, array $parameters = []): array
    {
        $columns = $this->client->describe($sql, $parameters);

        if ($this->telemetryConfig->options->traceQueries || $this->telemetryConfig->options->collectMetrics) {
            $this->logQuery($sql instanceof Sql ? $sql->toSql() : $sql, $parameters);
        }

        return $columns;
    }

    public function execute(Sql|string $sql, array $parameters = []): int
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($sql, $parameters, $query): int {
                $this->logQuery($query, $parameters);

                return $this->client->execute($sql, $parameters);
            },
            static fn(int $affected) => $affected,
        );
    }

    public function explain(Sql|string $sql, array $parameters = [], ?ExplainConfig $config = null): Plan
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery($query, $parameters, fn() => $this->client->explain($sql, $parameters, $config));
    }

    public function fetch(Sql|string $sql, array $parameters = []): ?array
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($sql, $parameters, $query): ?array {
                $this->logQuery($query, $parameters);

                return $this->client->fetch($sql, $parameters);
            },
            static fn(?array $row) => $row !== null ? 1 : 0,
        );
    }

    public function fetchAll(Sql|string $sql, array $parameters = []): array
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($sql, $parameters, $query): array {
                $this->logQuery($query, $parameters);

                return $this->client->fetchAll($sql, $parameters);
            },
            static fn(array $rows) => count($rows),
        );
    }

    public function fetchAllInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): array
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($mapper, $sql, $parameters, $query): array {
                $this->logQuery($query, $parameters);

                return $this->client->fetchAllInto($mapper, $sql, $parameters);
            },
            static fn(array $rows) => count($rows),
        );
    }

    public function fetchInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($mapper, $sql, $parameters, $query): mixed {
                $this->logQuery($query, $parameters);

                return $this->client->fetchInto($mapper, $sql, $parameters);
            },
            static fn(mixed $result) => $result !== null ? 1 : 0,
        );
    }

    public function fetchOne(Sql|string $sql, array $parameters = []): ?array
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($sql, $parameters, $query): ?array {
                $this->logQuery($query, $parameters);

                return $this->client->fetchOne($sql, $parameters);
            },
            static fn(?array $row) => $row !== null ? 1 : 0,
        );
    }

    public function fetchOneInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($mapper, $sql, $parameters, $query): mixed {
                $this->logQuery($query, $parameters);

                return $this->client->fetchOneInto($mapper, $sql, $parameters);
            },
            static fn(mixed $result) => $result !== null ? 1 : 0,
        );
    }

    public function fetchScalar(Sql|string $sql, array $parameters = []): mixed
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($sql, $parameters, $query): mixed {
                $this->logQuery($query, $parameters);

                return $this->client->fetchScalar($sql, $parameters);
            },
            static fn(mixed $value) => $value !== null ? 1 : 0,
        );
    }

    public function fetchScalarBool(Sql|string $sql, array $parameters = []): bool
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($sql, $parameters, $query): bool {
                $this->logQuery($query, $parameters);

                return $this->client->fetchScalarBool($sql, $parameters);
            },
            static fn(bool $value) => 1,
        );
    }

    public function fetchScalarFloat(Sql|string $sql, array $parameters = []): float
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($sql, $parameters, $query): float {
                $this->logQuery($query, $parameters);

                return $this->client->fetchScalarFloat($sql, $parameters);
            },
            static fn(float $value) => 1,
        );
    }

    public function fetchScalarInt(Sql|string $sql, array $parameters = []): int
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($sql, $parameters, $query): int {
                $this->logQuery($query, $parameters);

                return $this->client->fetchScalarInt($sql, $parameters);
            },
            static fn(int $value) => 1,
        );
    }

    public function fetchScalarString(Sql|string $sql, array $parameters = []): string
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($sql, $parameters, $query): string {
                $this->logQuery($query, $parameters);

                return $this->client->fetchScalarString($sql, $parameters);
            },
            static fn(string $value) => 1,
        );
    }

    public function fetchSingle(Sql|string $sql, array $parameters = []): array
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($sql, $parameters, $query): array {
                $this->logQuery($query, $parameters);

                return $this->client->fetchSingle($sql, $parameters);
            },
            static fn(array $row) => 1,
        );
    }

    public function fetchSingleInto(RowMapper $mapper, Sql|string $sql, array $parameters = []): mixed
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($mapper, $sql, $parameters, $query): mixed {
                $this->logQuery($query, $parameters);

                return $this->client->fetchSingleInto($mapper, $sql, $parameters);
            },
            static fn(mixed $result) => 1,
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
        $query = listen($channel)->toSql();

        $this->traceQuery(
            $query,
            [],
            function () use ($channel, $query): int {
                $this->logQuery($query, []);
                $this->client->listen($channel);

                return 0;
            },
            static fn(int $_) => 0,
        );
    }

    public function parameters(): ConnectionParameters
    {
        return $this->client->parameters();
    }

    public function rollBack(): void
    {
        $startTime = hrtime(true);
        $nestingLevel = $this->client->getTransactionNestingLevel();

        try {
            if ($this->telemetryConfig->options->transactionSpans === TransactionSpanMode::PER_OPERATION) {
                $this->traceTransactionOperation('ROLLBACK', $nestingLevel, fn() => $this->client->rollBack());
            } else {
                $this->client->rollBack();
                $this->completeAllTransactionSpans($nestingLevel);
            }

            $this->recordDuration($startTime, $this->buildTransactionMetricAttributes($nestingLevel, 'rollback'));
        } catch (Throwable $e) {
            $this->completeAllTransactionSpans($nestingLevel, $e);
            $this->recordDuration($startTime, $this->buildTransactionMetricAttributes($nestingLevel, 'rollback'));

            throw $e;
        }
    }

    public function setAutoCommit(bool $autoCommit): void
    {
        $this->client->setAutoCommit($autoCommit);
    }

    public function transaction(callable $callback): mixed
    {
        $this->beginTransaction();

        try {
            $returnValue = $callback($this);
            $this->commit();

            return $returnValue;
        } catch (Throwable $e) {
            $this->rollBack();

            throw $e;
        }
    }

    public function unlisten(string $channel): void
    {
        $query = unlisten($channel)->toSql();

        $this->traceQuery(
            $query,
            [],
            function () use ($channel, $query): int {
                $this->logQuery($query, []);
                $this->client->unlisten($channel);

                return 0;
            },
            static fn(int $_) => 0,
        );
    }

    public function wait(int $milliseconds): ?Notification
    {
        return $this->client->wait($milliseconds);
    }

    /**
     * @param list<mixed> $parameters
     *
     * @return array<string, array<bool|float|int|string>|bool|float|int|string>
     */
    private function buildQueryAttributes(string $query, array $parameters, QueryAttributes $queryAttrs): array
    {
        $attributes = [
            SemConvAttributes::DB_SYSTEM_NAME => PostgreSqlTelemetryAttributes::DB_SYSTEM_POSTGRESQL,
            SemConvAttributes::DB_NAMESPACE => $this->client->parameters()->database(),
            SemConvAttributes::SERVER_ADDRESS => $this->client->parameters()->host(),
        ];

        $port = $this->client->parameters()->port();

        if ($port !== 5432) {
            $attributes[SemConvAttributes::SERVER_PORT] = $port;
        }

        if ($queryAttrs->operation !== null) {
            $attributes[SemConvAttributes::DB_OPERATION_NAME] = $queryAttrs->operation;
        }

        if ($queryAttrs->target !== null) {
            $attributes[SemConvAttributes::DB_COLLECTION_NAME] = $queryAttrs->target;
        }

        $maxLength = $this->telemetryConfig->options->maxQueryLength;
        $queryText =
            $maxLength !== null && strlen($query) > $maxLength ? substr($query, 0, $maxLength) . '...' : $query;
        $attributes[SemConvAttributes::DB_QUERY_TEXT] = $queryText;

        if ($this->telemetryConfig->options->includeParameters && $parameters !== []) {
            $attributes = array_merge($attributes, $this->parameterFormatter->formatList(
                $parameters,
                $this->telemetryConfig->options->maxParameters,
                $this->telemetryConfig->options->maxParameterLength,
            ));
        }

        return $attributes;
    }

    /**
     * Low-cardinality attribute subset for metrics. Query text and parameter values are deliberately
     * excluded so each distinct SQL string does not spawn its own metric series.
     *
     * @return array<string, string>
     */
    private function buildQueryMetricAttributes(QueryAttributes $queryAttrs): array
    {
        $attributes = [
            SemConvAttributes::DB_SYSTEM_NAME => PostgreSqlTelemetryAttributes::DB_SYSTEM_POSTGRESQL,
            SemConvAttributes::DB_NAMESPACE => $this->client->parameters()->database(),
        ];

        if ($queryAttrs->operation !== null) {
            $attributes[SemConvAttributes::DB_OPERATION_NAME] = $queryAttrs->operation;
        }

        if ($queryAttrs->target !== null) {
            $attributes[SemConvAttributes::DB_COLLECTION_NAME] = $queryAttrs->target;
        }

        return $attributes;
    }

    private function buildSpanName(QueryAttributes $queryAttrs): string
    {
        if ($queryAttrs->operation !== null && $queryAttrs->target !== null) {
            return $queryAttrs->operation . ' ' . $queryAttrs->target;
        }

        if ($queryAttrs->operation !== null) {
            return $queryAttrs->operation;
        }

        return 'query';
    }

    /**
     * @return array<string, array<bool|float|int|string>|bool|float|int|string>
     */
    private function buildTransactionAttributes(int $nestingLevel): array
    {
        $attributes = [
            SemConvAttributes::DB_SYSTEM_NAME => PostgreSqlTelemetryAttributes::DB_SYSTEM_POSTGRESQL,
            SemConvAttributes::DB_NAMESPACE => $this->client->parameters()->database(),
            SemConvAttributes::SERVER_ADDRESS => $this->client->parameters()->host(),
            PostgreSqlTelemetryAttributes::DB_TRANSACTION_NESTING_LEVEL => $nestingLevel,
        ];

        $port = $this->client->parameters()->port();

        if ($port !== 5432) {
            $attributes[SemConvAttributes::SERVER_PORT] = $port;
        }

        if ($nestingLevel > 1) {
            $attributes[PostgreSqlTelemetryAttributes::DB_TRANSACTION_SAVEPOINT] = 'savepoint_' . ($nestingLevel - 1);
        }

        return $attributes;
    }

    /**
     * Low-cardinality attribute subset for the transaction duration metric. Excludes server.address and the
     * savepoint label so the metric stays low-cardinality.
     *
     * @return array<string, int|string>
     */
    private function buildTransactionMetricAttributes(int $nestingLevel, string $operation): array
    {
        return [
            SemConvAttributes::DB_SYSTEM_NAME => PostgreSqlTelemetryAttributes::DB_SYSTEM_POSTGRESQL,
            SemConvAttributes::DB_NAMESPACE => $this->client->parameters()->database(),
            SemConvAttributes::DB_OPERATION_NAME => $operation,
            PostgreSqlTelemetryAttributes::DB_TRANSACTION_NESTING_LEVEL => $nestingLevel,
        ];
    }

    private function buildTransactionSpanName(string $operation, int $nestingLevel): string
    {
        if ($nestingLevel > 1) {
            return $operation . ' SAVEPOINT';
        }

        return $operation . ' TRANSACTION';
    }

    private function completeAllTransactionSpans(int $fromLevel, ?Throwable $exception = null): void
    {
        for ($level = $fromLevel; $level >= 1; $level--) {
            $this->completeTransactionSpan($level, $exception);
        }
    }

    /**
     * @param \Closure(): void $execute
     */
    private function traceTransactionOperation(string $operation, int $nestingLevel, Closure $execute): void
    {
        $tracer = $this->tracer;

        if ($tracer === null) {
            $execute();

            return;
        }

        $span = $tracer->span(
            $this->buildTransactionSpanName($operation, $nestingLevel),
            SpanKind::CLIENT,
            $this->buildTransactionAttributes($nestingLevel),
        );

        try {
            $execute();
        } catch (Throwable $e) {
            $this->recordFailure($span, $e);

            throw $e;
        } finally {
            $tracer->complete($span);
        }
    }

    private function completeTransactionSpan(int $nestingLevel, ?Throwable $exception = null): void
    {
        $tracer = $this->tracer;

        if (!isset($this->transactionSpans[$nestingLevel]) || $tracer === null) {
            return;
        }

        $span = $this->transactionSpans[$nestingLevel]['span'];

        // OTEL spec: instrumentation leaves the status Unset on success; only errors set a status.
        if ($exception !== null) {
            $this->recordFailure($span, $exception);
        }

        $this->transactionSpans[$nestingLevel]['scope']->detach();
        $tracer->complete($span);
        unset($this->transactionSpans[$nestingLevel]);
    }

    /**
     * @param list<mixed> $parameters
     */
    private function logQuery(string $query, array $parameters): void
    {
        if ($this->logger === null) {
            return;
        }

        $queryAttrs = $this->queryAttributesExtractor->extract($query);

        $this->logger->debug('Executing query', $this->buildQueryAttributes($query, $parameters, $queryAttrs));
    }

    /**
     * @param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes
     */
    private function recordDuration(int|float|false $startTime, array $attributes): void
    {
        if ($this->operationDuration === null || $startTime === false) {
            return;
        }

        $now = hrtime(true);

        if ($now === false) {
            return;
        }

        $duration = ($now - $startTime) / 1_000_000_000;
        $this->operationDuration->record($duration, $attributes);
    }

    private function recordFailure(Span $span, Throwable $e): void
    {
        $span->recordException($e, $this->telemetryConfig->clock->now());
        $span->setAttribute(SemConvAttributes::ERROR_TYPE, $e::class);

        if ($e instanceof QueryException) {
            $span->setAttribute(SemConvAttributes::DB_RESPONSE_STATUS_CODE, $e->error()->sqlState);
        }

        $span->setStatus(SpanStatus::error($e->getMessage()));
    }

    private function recordRowCount(int $rowCount, QueryAttributes $queryAttrs): void
    {
        if ($this->returnedRows === null) {
            return;
        }

        $this->returnedRows->record($rowCount, $this->buildQueryMetricAttributes($queryAttrs));
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
    private function traceQuery(
        string $query,
        array $parameters,
        callable $operation,
        ?callable $rowCountExtractor = null,
    ): mixed {
        $startTime = hrtime(true);
        $queryAttrs = $this->queryAttributesExtractor->extract($query);
        $metricAttributes = $this->buildQueryMetricAttributes($queryAttrs);
        $span = null;

        if ($this->telemetryConfig->options->traceQueries && $this->tracer !== null) {
            $span = $this->tracer->span(
                $this->buildSpanName($queryAttrs),
                SpanKind::CLIENT,
                $this->buildQueryAttributes($query, $parameters, $queryAttrs),
            );
        }

        try {
            $result = $operation();

            if ($span !== null && $rowCountExtractor !== null) {
                $rowCount = $rowCountExtractor($result);
                $span->setAttribute(SemConvAttributes::DB_RESPONSE_RETURNED_ROWS, $rowCount);
                $this->recordRowCount($rowCount, $queryAttrs);
            }
            // OTEL spec: instrumentation leaves the status Unset on success.

            $this->recordDuration($startTime, $metricAttributes);

            return $result;
        } catch (Throwable $e) {
            if ($span !== null) {
                $this->recordFailure($span, $e);
            }

            $this->recordDuration($startTime, $metricAttributes);

            throw $e;
        } finally {
            if ($span !== null && $this->tracer !== null) {
                $this->tracer->complete($span);
            }
        }
    }
}
