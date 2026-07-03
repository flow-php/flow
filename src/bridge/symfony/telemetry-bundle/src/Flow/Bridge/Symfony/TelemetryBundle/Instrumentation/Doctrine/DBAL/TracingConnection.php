<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL;

use Closure;
use Doctrine\DBAL\Driver\Connection as ConnectionInterface;
use Doctrine\DBAL\Driver\Middleware\AbstractConnectionMiddleware;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement as DriverStatement;
use Flow\Telemetry\SemConvAttributes;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Override;
use Throwable;

use function array_key_exists;
use function hrtime;
use function preg_match;
use function preg_quote;

final class TracingConnection extends AbstractConnectionMiddleware
{
    /**
     * The open span for the current transaction in GROUPED mode, held between
     * beginTransaction() and commit()/rollBack() so query spans nest under it.
     */
    private ?Span $transactionSpan = null;

    /**
     * @param array<string, int|string> $transactionAttributes db.system.name, db.namespace, server.address,
     *                                                          server.port, db.connection.name
     * @param list<string> $excludeTables queries referencing any of these tables are not traced, so
     *                                     high-churn internals (e.g. the cache_items table behind a
     *                                     Doctrine DBAL cache pool) do not surface as orphan spans
     */
    public function __construct(
        ConnectionInterface $connection,
        private readonly QueryTracer $queryTracer,
        private readonly TransactionSpanMode $transactionSpanMode = TransactionSpanMode::GROUPED,
        private readonly array $transactionAttributes = [],
        private readonly array $excludeTables = [],
    ) {
        parent::__construct($connection);
    }

    public function __destruct()
    {
        if ($this->transactionSpan === null) {
            return;
        }

        $span = $this->transactionSpan;
        $this->transactionSpan = null;
        $span->setStatus(SpanStatus::error('Transaction was neither committed nor rolled back'));
        $this->queryTracer->tracer()->complete($span);
    }

    #[Override]
    public function beginTransaction(): void
    {
        $startTime = hrtime(true);

        try {
            if ($this->transactionSpanMode === TransactionSpanMode::OFF) {
                parent::beginTransaction();

                return;
            }

            if ($this->transactionSpanMode === TransactionSpanMode::PER_OPERATION) {
                $this->traceTransactionOperation('BEGIN TRANSACTION', 'begin', fn() => parent::beginTransaction());

                return;
            }

            $tracer = $this->queryTracer->tracer();
            $span = $tracer->span('BEGIN TRANSACTION', SpanKind::CLIENT, $this->transactionSpanAttributes());

            try {
                parent::beginTransaction();
                $this->transactionSpan = $span;
            } catch (Throwable $exception) {
                $this->queryTracer->recordError($span, $exception);
                $tracer->complete($span);

                throw $exception;
            }
        } finally {
            $this->queryTracer->recordDuration($startTime, $this->transactionMetricAttributes('begin'));
        }
    }

    #[Override]
    public function commit(): void
    {
        $this->completeTransaction('COMMIT TRANSACTION', 'commit', fn() => parent::commit());
    }

    #[Override]
    public function rollBack(): void
    {
        $this->completeTransaction('ROLLBACK TRANSACTION', 'rollback', fn() => parent::rollBack());
    }

    #[Override]
    public function exec(string $sql): int|string
    {
        if ($this->isExcluded($sql)) {
            return parent::exec($sql);
        }

        return $this->traceQuery(
            $sql,
            fn() => parent::exec($sql),
            static fn(int|string $affected): int => (int) $affected,
        );
    }

    #[Override]
    public function prepare(string $sql): DriverStatement
    {
        if ($this->isExcluded($sql)) {
            return parent::prepare($sql);
        }

        $sqlAttributes = $this->queryTracer->extract($sql);
        $tracer = $this->queryTracer->tracer();
        $span = $tracer->span(
            $this->queryTracer->spanName($sqlAttributes),
            SpanKind::CLIENT,
            $this->queryTracer->queryAttributes($sql, $sqlAttributes),
        );

        try {
            return new TracingStatement(parent::prepare($sql), $this->queryTracer, $sql, $sqlAttributes);
        } catch (Throwable $exception) {
            $this->queryTracer->recordError($span, $exception);

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }

    #[Override]
    public function query(string $sql): Result
    {
        if ($this->isExcluded($sql)) {
            return parent::query($sql);
        }

        return $this->traceQuery(
            $sql,
            fn() => parent::query($sql),
            static fn(Result $result): int => (int) $result->rowCount(),
        );
    }

    /**
     * @template T
     *
     * @param \Closure(): T $execute
     * @param \Closure(T): int $rowCount
     *
     * @return T
     */
    private function traceQuery(string $sql, Closure $execute, Closure $rowCount): mixed
    {
        $sqlAttributes = $this->queryTracer->extract($sql);
        $startTime = hrtime(true);
        $tracer = $this->queryTracer->tracer();
        $span = $tracer->span(
            $this->queryTracer->spanName($sqlAttributes),
            SpanKind::CLIENT,
            $this->queryTracer->queryAttributes($sql, $sqlAttributes),
        );

        try {
            $result = $execute();
            $rows = $rowCount($result);
            $span->setAttribute(SemConvAttributes::DB_RESPONSE_RETURNED_ROWS, $rows);
            $this->queryTracer->recordQueryMetrics($startTime, $rows, $sqlAttributes);

            return $result;
        } catch (Throwable $exception) {
            $this->queryTracer->recordError($span, $exception);
            $this->queryTracer->recordQueryMetrics($startTime, null, $sqlAttributes);

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }

    /**
     * @param \Closure(): void $execute
     */
    private function completeTransaction(string $operationSpanName, string $operation, Closure $execute): void
    {
        $startTime = hrtime(true);

        try {
            if ($this->transactionSpanMode === TransactionSpanMode::OFF) {
                $execute();

                return;
            }

            if ($this->transactionSpanMode === TransactionSpanMode::PER_OPERATION) {
                $this->traceTransactionOperation($operationSpanName, $operation, $execute);

                return;
            }

            $span = $this->transactionSpan;
            $this->transactionSpan = null;

            if ($span === null) {
                $execute();

                return;
            }

            $tracer = $this->queryTracer->tracer();

            try {
                $execute();
            } catch (Throwable $exception) {
                $this->queryTracer->recordError($span, $exception);

                throw $exception;
            } finally {
                $tracer->complete($span);
            }
        } finally {
            $this->queryTracer->recordDuration($startTime, $this->transactionMetricAttributes($operation));
        }
    }

    /**
     * @param \Closure(): void $execute
     */
    private function traceTransactionOperation(string $spanName, string $operation, Closure $execute): void
    {
        $tracer = $this->queryTracer->tracer();
        $span = $tracer->span($spanName, SpanKind::CLIENT, [SemConvAttributes::DB_OPERATION_NAME => $operation]
        + $this->transactionSpanAttributes());

        try {
            $execute();
        } catch (Throwable $exception) {
            $this->queryTracer->recordError($span, $exception);

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }

    /**
     * @return array<string, int|string>
     */
    private function transactionSpanAttributes(): array
    {
        return $this->transactionAttributes + [DbAttributes::DB_TRANSACTION_NESTING_LEVEL => 1];
    }

    /**
     * @return array<string, int|string>
     */
    private function transactionMetricAttributes(string $operation): array
    {
        $attributes = [
            SemConvAttributes::DB_OPERATION_NAME => $operation,
            DbAttributes::DB_TRANSACTION_NESTING_LEVEL => 1,
        ];

        foreach ([SemConvAttributes::DB_SYSTEM_NAME, SemConvAttributes::DB_NAMESPACE] as $key) {
            if (array_key_exists($key, $this->transactionAttributes)) {
                $attributes[$key] = $this->transactionAttributes[$key];
            }
        }

        return $attributes;
    }

    private function isExcluded(string $sql): bool
    {
        foreach ($this->excludeTables as $table) {
            if (preg_match('/\b' . preg_quote($table, '/') . '\b/i', $sql) === 1) {
                return true;
            }
        }

        return false;
    }
}
