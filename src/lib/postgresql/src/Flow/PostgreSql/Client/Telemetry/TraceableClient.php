<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Telemetry;

use function Flow\PostgreSql\DSL\{listen, unlisten};
use Flow\PostgreSql\AST\Transformers\ExplainConfig;
use Flow\PostgreSql\Client\{Client, ConnectionParameters, Cursor, Notification, RowMapper};
use Flow\PostgreSql\Client\Exception\QueryException;
use Flow\PostgreSql\Client\Types\ValueConverters;
use Flow\PostgreSql\Explain\Plan\Plan;
use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\Telemetry\Logger\Logger;
use Flow\Telemetry\Meter\Instrument\Histogram;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Tracer\{Span, SpanKind, SpanStatus, Tracer};

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
     * @var array<int, Span>
     */
    private array $transactionSpans = [];

    public function __construct(
        private readonly Client $client,
        private readonly PostgreSqlTelemetryConfig $telemetryConfig,
    ) {
        $this->queryAttributesExtractor = new QueryAttributesExtractor();
        $this->parameterFormatter = new ParameterFormatter();

        if ($this->telemetryConfig->options->traceQueries || $this->telemetryConfig->options->traceTransactions) {
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
                'operation_duration',
                's',
                'Duration of database client operations',
            );
            $this->returnedRows = $meter->createHistogram(
                'response_returned_rows',
                '{row}',
                'Number of rows returned by database operations',
            );
        }

        if ($this->telemetryConfig->options->logQueries) {
            $this->logger = $telemetryConfig->telemetry->logger(
                'flow_php_postgresql',
                PackageVersion::get('flow-php/postgresql'),
            );
        }
    }

    public function beginTransaction() : void
    {
        $startTime = \hrtime(true);
        $nestingLevel = $this->client->getTransactionNestingLevel() + 1;

        try {
            $this->client->beginTransaction();

            if ($this->telemetryConfig->options->traceTransactions && $this->tracer !== null) {
                $span = $this->tracer->span(
                    $this->buildTransactionSpanName('BEGIN', $nestingLevel),
                    SpanKind::CLIENT,
                    $this->buildTransactionAttributes($nestingLevel),
                );
                $this->transactionSpans[$nestingLevel] = $span;
            }

            $this->recordDuration($startTime, $this->buildTransactionAttributes($nestingLevel));
        } catch (\Throwable $e) {
            $this->recordDuration($startTime, $this->buildTransactionAttributes($nestingLevel));

            throw $e;
        }
    }

    public function close() : void
    {
        $this->client->close();
    }

    public function commit() : void
    {
        $startTime = \hrtime(true);
        $nestingLevel = $this->client->getTransactionNestingLevel();

        try {
            $this->client->commit();

            $this->completeTransactionSpan($nestingLevel, SpanStatus::ok());
            $this->recordDuration($startTime, $this->buildTransactionAttributes($nestingLevel));
        } catch (\Throwable $e) {
            $this->completeTransactionSpan($nestingLevel, SpanStatus::error($e->getMessage()), $e);
            $this->recordDuration($startTime, $this->buildTransactionAttributes($nestingLevel));

            throw $e;
        }
    }

    public function converters() : ValueConverters
    {
        return $this->client->converters();
    }

    public function cursor(Sql|string $sql, array $parameters = []) : Cursor
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;
        $cursor = $this->client->cursor($sql, $parameters);

        if (!$this->telemetryConfig->options->traceQueries && !$this->telemetryConfig->options->collectMetrics) {
            return $cursor;
        }

        $this->logQuery($query, $parameters);

        return new TraceableCursor($cursor, $this->telemetryConfig, $this->client->parameters(), $query, $parameters);
    }

    public function execute(Sql|string $sql, array $parameters = []) : int
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($sql, $parameters, $query) : int {
                $this->logQuery($query, $parameters);

                return $this->client->execute($sql, $parameters);
            },
            static fn (int $affected) => $affected,
        );
    }

    public function explain(Sql|string $sql, array $parameters = [], ?ExplainConfig $config = null) : Plan
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            fn () => $this->client->explain($sql, $parameters, $config),
        );
    }

    public function fetch(Sql|string $sql, array $parameters = []) : ?array
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($sql, $parameters, $query) : ?array {
                $this->logQuery($query, $parameters);

                return $this->client->fetch($sql, $parameters);
            },
            static fn (?array $row) => $row !== null ? 1 : 0,
        );
    }

    public function fetchAll(Sql|string $sql, array $parameters = []) : array
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($sql, $parameters, $query) : array {
                $this->logQuery($query, $parameters);

                return $this->client->fetchAll($sql, $parameters);
            },
            static fn (array $rows) => \count($rows),
        );
    }

    public function fetchAllInto(
        RowMapper $mapper,
        Sql|string $sql,
        array $parameters = [],
    ) : array {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($mapper, $sql, $parameters, $query) : array {
                $this->logQuery($query, $parameters);

                return $this->client->fetchAllInto($mapper, $sql, $parameters);
            },
            static fn (array $rows) => \count($rows),
        );
    }

    public function fetchInto(
        RowMapper $mapper,
        Sql|string $sql,
        array $parameters = [],
    ) : mixed {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($mapper, $sql, $parameters, $query) : mixed {
                $this->logQuery($query, $parameters);

                return $this->client->fetchInto($mapper, $sql, $parameters);
            },
            static fn (mixed $result) => $result !== null ? 1 : 0,
        );
    }

    public function fetchOne(Sql|string $sql, array $parameters = []) : ?array
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($sql, $parameters, $query) : ?array {
                $this->logQuery($query, $parameters);

                return $this->client->fetchOne($sql, $parameters);
            },
            static fn (?array $row) => $row !== null ? 1 : 0,
        );
    }

    public function fetchOneInto(
        RowMapper $mapper,
        Sql|string $sql,
        array $parameters = [],
    ) : mixed {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($mapper, $sql, $parameters, $query) : mixed {
                $this->logQuery($query, $parameters);

                return $this->client->fetchOneInto($mapper, $sql, $parameters);
            },
            static fn (mixed $result) => $result !== null ? 1 : 0,
        );
    }

    public function fetchScalar(Sql|string $sql, array $parameters = []) : mixed
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($sql, $parameters, $query) : mixed {
                $this->logQuery($query, $parameters);

                return $this->client->fetchScalar($sql, $parameters);
            },
            static fn (mixed $value) => $value !== null ? 1 : 0,
        );
    }

    public function fetchScalarBool(Sql|string $sql, array $parameters = []) : bool
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($sql, $parameters, $query) : bool {
                $this->logQuery($query, $parameters);

                return $this->client->fetchScalarBool($sql, $parameters);
            },
            static fn (bool $value) => 1,
        );
    }

    public function fetchScalarFloat(Sql|string $sql, array $parameters = []) : float
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($sql, $parameters, $query) : float {
                $this->logQuery($query, $parameters);

                return $this->client->fetchScalarFloat($sql, $parameters);
            },
            static fn (float $value) => 1,
        );
    }

    public function fetchScalarInt(Sql|string $sql, array $parameters = []) : int
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($sql, $parameters, $query) : int {
                $this->logQuery($query, $parameters);

                return $this->client->fetchScalarInt($sql, $parameters);
            },
            static fn (int $value) => 1,
        );
    }

    public function fetchScalarString(Sql|string $sql, array $parameters = []) : string
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($sql, $parameters, $query) : string {
                $this->logQuery($query, $parameters);

                return $this->client->fetchScalarString($sql, $parameters);
            },
            static fn (string $value) => 1,
        );
    }

    public function fetchSingle(Sql|string $sql, array $parameters = []) : array
    {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($sql, $parameters, $query) : array {
                $this->logQuery($query, $parameters);

                return $this->client->fetchSingle($sql, $parameters);
            },
            static fn (array $row) => 1,
        );
    }

    public function fetchSingleInto(
        RowMapper $mapper,
        Sql|string $sql,
        array $parameters = [],
    ) : mixed {
        $query = $sql instanceof Sql ? $sql->toSql() : $sql;

        return $this->traceQuery(
            $query,
            $parameters,
            function () use ($mapper, $sql, $parameters, $query) : mixed {
                $this->logQuery($query, $parameters);

                return $this->client->fetchSingleInto($mapper, $sql, $parameters);
            },
            static fn (mixed $result) => 1,
        );
    }

    public function getTransactionNestingLevel() : int
    {
        return $this->client->getTransactionNestingLevel();
    }

    public function isAutoCommit() : bool
    {
        return $this->client->isAutoCommit();
    }

    public function isConnected() : bool
    {
        return $this->client->isConnected();
    }

    public function lastInsertId(string $sequenceName) : int|string
    {
        return $this->client->lastInsertId($sequenceName);
    }

    public function listen(string $channel) : void
    {
        $query = listen($channel)->toSql();

        $this->traceQuery(
            $query,
            [],
            function () use ($channel, $query) : int {
                $this->logQuery($query, []);
                $this->client->listen($channel);

                return 0;
            },
            static fn (int $_) => 0,
        );
    }

    public function parameters() : ConnectionParameters
    {
        return $this->client->parameters();
    }

    public function rollBack() : void
    {
        $startTime = \hrtime(true);
        $nestingLevel = $this->client->getTransactionNestingLevel();

        try {
            $this->client->rollBack();

            $this->completeAllTransactionSpans($nestingLevel, SpanStatus::ok());
            $this->recordDuration($startTime, $this->buildTransactionAttributes($nestingLevel));
        } catch (\Throwable $e) {
            $this->completeAllTransactionSpans($nestingLevel, SpanStatus::error($e->getMessage()), $e);
            $this->recordDuration($startTime, $this->buildTransactionAttributes($nestingLevel));

            throw $e;
        }
    }

    public function setAutoCommit(bool $autoCommit) : void
    {
        $this->client->setAutoCommit($autoCommit);
    }

    public function transaction(callable $callback) : mixed
    {
        $this->beginTransaction();

        try {
            $returnValue = $callback($this);
            $this->commit();

            return $returnValue;
        } catch (\Throwable $e) {
            $this->rollBack();

            throw $e;
        }
    }

    public function unlisten(string $channel) : void
    {
        $query = unlisten($channel)->toSql();

        $this->traceQuery(
            $query,
            [],
            function () use ($channel, $query) : int {
                $this->logQuery($query, []);
                $this->client->unlisten($channel);

                return 0;
            },
            static fn (int $_) => 0,
        );
    }

    public function wait(int $milliseconds) : ?Notification
    {
        return $this->client->wait($milliseconds);
    }

    /**
     * @param list<mixed> $parameters
     *
     * @return array<string, array<bool|float|int|string>|bool|float|int|string>
     */
    private function buildQueryAttributes(string $query, array $parameters, QueryAttributes $queryAttrs) : array
    {
        $attributes = [
            PostgreSqlTelemetryAttributes::DB_SYSTEM_NAME => PostgreSqlTelemetryAttributes::DB_SYSTEM_POSTGRESQL,
            PostgreSqlTelemetryAttributes::DB_NAMESPACE => $this->client->parameters()->database(),
            PostgreSqlTelemetryAttributes::SERVER_ADDRESS => $this->client->parameters()->host(),
        ];

        $port = $this->client->parameters()->port();

        if ($port !== 5432) {
            $attributes[PostgreSqlTelemetryAttributes::SERVER_PORT] = $port;
        }

        if ($queryAttrs->operation !== null) {
            $attributes[PostgreSqlTelemetryAttributes::DB_OPERATION_NAME] = $queryAttrs->operation;
        }

        if ($queryAttrs->target !== null) {
            $attributes[PostgreSqlTelemetryAttributes::DB_COLLECTION_NAME] = $queryAttrs->target;
        }

        $maxLength = $this->telemetryConfig->options->maxQueryLength;
        $queryText = ($maxLength !== null && \strlen($query) > $maxLength)
            ? \substr($query, 0, $maxLength) . '...'
            : $query;
        $attributes[PostgreSqlTelemetryAttributes::DB_QUERY_TEXT] = $queryText;

        if ($this->telemetryConfig->options->includeParameters && $parameters !== []) {
            $maxParams = $this->telemetryConfig->options->maxParameters;
            $maxParamLength = $this->telemetryConfig->options->maxParameterLength;
            $count = 0;

            foreach ($parameters as $index => $value) {
                if ($maxParams !== null && $count >= $maxParams) {
                    break;
                }
                $key = PostgreSqlTelemetryAttributes::DB_QUERY_PARAMETER_PREFIX . ($index + 1);
                $attributes[$key] = $this->parameterFormatter->format($value, $maxParamLength);
                $count++;
            }
        }

        return $attributes;
    }

    private function buildSpanName(QueryAttributes $queryAttrs) : string
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
    private function buildTransactionAttributes(int $nestingLevel) : array
    {
        $attributes = [
            PostgreSqlTelemetryAttributes::DB_SYSTEM_NAME => PostgreSqlTelemetryAttributes::DB_SYSTEM_POSTGRESQL,
            PostgreSqlTelemetryAttributes::DB_NAMESPACE => $this->client->parameters()->database(),
            PostgreSqlTelemetryAttributes::SERVER_ADDRESS => $this->client->parameters()->host(),
            PostgreSqlTelemetryAttributes::DB_TRANSACTION_NESTING_LEVEL => $nestingLevel,
        ];

        $port = $this->client->parameters()->port();

        if ($port !== 5432) {
            $attributes[PostgreSqlTelemetryAttributes::SERVER_PORT] = $port;
        }

        if ($nestingLevel > 1) {
            $attributes[PostgreSqlTelemetryAttributes::DB_TRANSACTION_SAVEPOINT] = 'savepoint_' . ($nestingLevel - 1);
        }

        return $attributes;
    }

    private function buildTransactionSpanName(string $operation, int $nestingLevel) : string
    {
        if ($nestingLevel > 1) {
            return $operation . ' SAVEPOINT';
        }

        return $operation . ' TRANSACTION';
    }

    private function completeAllTransactionSpans(int $fromLevel, SpanStatus $status, ?\Throwable $exception = null) : void
    {
        for ($level = $fromLevel; $level >= 1; $level--) {
            $this->completeTransactionSpan($level, $status, $exception);
        }
    }

    private function completeTransactionSpan(int $nestingLevel, SpanStatus $status, ?\Throwable $exception = null) : void
    {
        $tracer = $this->tracer;

        if (!isset($this->transactionSpans[$nestingLevel]) || $tracer === null) {
            return;
        }

        $span = $this->transactionSpans[$nestingLevel];

        if ($exception !== null) {
            $this->recordFailure($span, $exception);
        } else {
            $span->setStatus($status);
        }

        $tracer->complete($span);
        unset($this->transactionSpans[$nestingLevel]);
    }

    /**
     * @param list<mixed> $parameters
     */
    private function logQuery(string $query, array $parameters) : void
    {
        if ($this->logger === null) {
            return;
        }

        $queryAttrs = $this->queryAttributesExtractor->extract($query);

        $this->logger->debug(
            'Executing query',
            $this->buildQueryAttributes($query, $parameters, $queryAttrs),
        );
    }

    /**
     * @param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes
     */
    private function recordDuration(int $startTime, array $attributes) : void
    {
        if ($this->operationDuration === null) {
            return;
        }

        $duration = (\hrtime(true) - $startTime) / 1_000_000_000;
        $this->operationDuration->record($duration, $attributes);
    }

    private function recordFailure(Span $span, \Throwable $e) : void
    {
        $span->recordException($e, $this->telemetryConfig->clock->now());
        $span->setAttribute(PostgreSqlTelemetryAttributes::ERROR_TYPE, $e::class);

        if ($e instanceof QueryException) {
            $span->setAttribute(
                PostgreSqlTelemetryAttributes::DB_RESPONSE_STATUS_CODE,
                $e->error()->sqlState,
            );
        }

        $span->setStatus(SpanStatus::error($e->getMessage()));
    }

    private function recordRowCount(int $rowCount, QueryAttributes $queryAttrs) : void
    {
        if ($this->returnedRows === null) {
            return;
        }

        $attributes = [
            PostgreSqlTelemetryAttributes::DB_SYSTEM_NAME => PostgreSqlTelemetryAttributes::DB_SYSTEM_POSTGRESQL,
            PostgreSqlTelemetryAttributes::DB_NAMESPACE => $this->client->parameters()->database(),
        ];

        if ($queryAttrs->operation !== null) {
            $attributes[PostgreSqlTelemetryAttributes::DB_OPERATION_NAME] = $queryAttrs->operation;
        }

        $this->returnedRows->record($rowCount, $attributes);
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
    private function traceQuery(string $query, array $parameters, callable $operation, ?callable $rowCountExtractor = null) : mixed
    {
        $startTime = \hrtime(true);
        $queryAttrs = $this->queryAttributesExtractor->extract($query);
        $attributes = $this->buildQueryAttributes($query, $parameters, $queryAttrs);
        $span = null;

        if ($this->telemetryConfig->options->traceQueries && $this->tracer !== null) {
            $span = $this->tracer->span(
                $this->buildSpanName($queryAttrs),
                SpanKind::CLIENT,
                $attributes,
            );
        }

        try {
            $result = $operation();

            if ($span !== null) {
                if ($rowCountExtractor !== null) {
                    $rowCount = $rowCountExtractor($result);
                    $span->setAttribute(PostgreSqlTelemetryAttributes::DB_RESPONSE_RETURNED_ROWS, $rowCount);
                    $this->recordRowCount($rowCount, $queryAttrs);
                }

                $span->setStatus(SpanStatus::ok());
            }

            $this->recordDuration($startTime, $attributes);

            return $result;
        } catch (\Throwable $e) {
            if ($span !== null) {
                $this->recordFailure($span, $e);
            }

            $this->recordDuration($startTime, $attributes);

            throw $e;
        } finally {
            if ($span !== null && $this->tracer !== null) {
                $this->tracer->complete($span);
            }
        }
    }
}
