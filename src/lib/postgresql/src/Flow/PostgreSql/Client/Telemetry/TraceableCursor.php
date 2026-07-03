<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Telemetry;

use Flow\PostgreSql\Client\ConnectionParameters;
use Flow\PostgreSql\Client\Cursor;
use Flow\PostgreSql\Client\RowMapper;
use Flow\Telemetry\Meter\Instrument\Histogram;
use Flow\Telemetry\Meter\Meter;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\SemConvAttributes;
use Flow\Telemetry\SemConvMetrics;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\Tracer;
use Generator;
use Throwable;
use Traversable;

use function array_merge;
use function strlen;
use function substr;

/**
 * Decorator that adds telemetry instrumentation to a PostgreSQL cursor.
 *
 * Tracks row iteration with spans and metrics, completing telemetry
 * when the cursor is freed or iteration completes.
 */
final class TraceableCursor implements Cursor
{
    private ?Meter $meter = null;

    private readonly ParameterFormatter $parameterFormatter;

    private readonly QueryAttributesExtractor $queryAttributesExtractor;

    private readonly QueryAttributes $queryAttrs;

    private ?Histogram $returnedRows = null;

    private int $rowsIterated = 0;

    private ?Span $span = null;

    private ?Tracer $tracer = null;

    /**
     * @param list<mixed> $parameters
     */
    public function __construct(
        private readonly Cursor $cursor,
        private readonly PostgreSqlTelemetryConfig $telemetryConfig,
        private readonly ConnectionParameters $connectionParameters,
        private readonly string $query,
        private readonly array $parameters = [],
    ) {
        $this->queryAttributesExtractor = new QueryAttributesExtractor();
        $this->parameterFormatter = new ParameterFormatter();
        $this->queryAttrs = $this->queryAttributesExtractor->extract($query);

        if ($this->telemetryConfig->options->traceQueries) {
            $this->tracer = $telemetryConfig->telemetry->tracer(
                'flow_php_postgresql',
                PackageVersion::get('flow-php/postgresql'),
            );

            $this->span = $this->tracer->span($this->buildSpanName(), SpanKind::CLIENT, $this->buildQueryAttributes());
        }

        if ($this->telemetryConfig->options->collectMetrics) {
            $this->meter = $telemetryConfig->telemetry->meter(
                'flow_php_postgresql',
                PackageVersion::get('flow-php/postgresql'),
            );
            $this->returnedRows = $this->meter->createHistogram(
                SemConvMetrics::DB_CLIENT_RESPONSE_RETURNED_ROWS,
                '{row}',
                'Number of rows returned by database operations',
                [1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0],
            );
        }
    }

    public function count(): int
    {
        return $this->cursor->count();
    }

    public function free(): void
    {
        try {
            $this->cursor->free();
            $this->completeSpan();
        } catch (Throwable $e) {
            $this->completeSpan($e);

            throw $e;
        }
    }

    public function getIterator(): Traversable
    {
        return $this->iterate();
    }

    /**
     * @return \Generator<int, array<string, mixed>>
     */
    public function iterate(): Generator
    {
        try {
            foreach ($this->cursor->iterate() as $row) {
                $this->rowsIterated++;

                yield $row;
            }

            $this->completeSpan();
        } catch (Throwable $e) {
            $this->completeSpan($e);

            throw $e;
        }
    }

    /**
     * @template T
     *
     * @param RowMapper<T> $mapper
     *
     * @return \Generator<int, T>
     */
    public function map(RowMapper $mapper): Generator
    {
        try {
            foreach ($this->cursor->map($mapper) as $object) {
                $this->rowsIterated++;

                yield $object;
            }

            $this->completeSpan();
        } catch (Throwable $e) {
            $this->completeSpan($e);

            throw $e;
        }
    }

    public function next(): ?array
    {
        $row = $this->cursor->next();

        if ($row !== null) {
            $this->rowsIterated++;
        }

        return $row;
    }

    /**
     * @return array<string, array<bool|float|int|string>|bool|float|int|string>
     */
    private function buildQueryAttributes(): array
    {
        $attributes = [
            SemConvAttributes::DB_SYSTEM_NAME => PostgreSqlTelemetryAttributes::DB_SYSTEM_POSTGRESQL,
            SemConvAttributes::DB_NAMESPACE => $this->connectionParameters->database(),
            SemConvAttributes::SERVER_ADDRESS => $this->connectionParameters->host(),
        ];

        $port = $this->connectionParameters->port();

        if ($port !== 5432) {
            $attributes[SemConvAttributes::SERVER_PORT] = $port;
        }

        if ($this->queryAttrs->operation !== null) {
            $attributes[SemConvAttributes::DB_OPERATION_NAME] = $this->queryAttrs->operation;
        }

        if ($this->queryAttrs->target !== null) {
            $attributes[SemConvAttributes::DB_COLLECTION_NAME] = $this->queryAttrs->target;
        }

        $maxLength = $this->telemetryConfig->options->maxQueryLength;
        $queryText =
            $maxLength !== null && strlen($this->query) > $maxLength
                ? substr($this->query, 0, $maxLength) . '...'
                : $this->query;
        $attributes[SemConvAttributes::DB_QUERY_TEXT] = $queryText;

        if ($this->telemetryConfig->options->includeParameters && $this->parameters !== []) {
            $attributes = array_merge($attributes, $this->parameterFormatter->formatList(
                $this->parameters,
                $this->telemetryConfig->options->maxParameters,
                $this->telemetryConfig->options->maxParameterLength,
            ));
        }

        return $attributes;
    }

    private function buildSpanName(): string
    {
        if ($this->queryAttrs->operation !== null && $this->queryAttrs->target !== null) {
            return $this->queryAttrs->operation . ' ' . $this->queryAttrs->target . ' (cursor)';
        }

        if ($this->queryAttrs->operation !== null) {
            return $this->queryAttrs->operation . ' (cursor)';
        }

        return 'cursor';
    }

    private function completeSpan(?Throwable $exception = null): void
    {
        $span = $this->span;

        if ($span === null) {
            $this->recordMetrics();

            return;
        }

        $span->setAttribute(SemConvAttributes::DB_RESPONSE_RETURNED_ROWS, $this->rowsIterated);

        // OTEL spec: instrumentation leaves the status Unset on success; only errors set a status.
        if ($exception !== null) {
            $span->recordException($exception, $this->telemetryConfig->clock->now());
            $span->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));
        }

        $tracer = $this->tracer;

        if ($tracer !== null) {
            $tracer->complete($span);
        }

        $this->recordMetrics();
        $this->span = null;
    }

    private function recordMetrics(): void
    {
        if ($this->returnedRows === null) {
            return;
        }

        $attributes = [
            SemConvAttributes::DB_SYSTEM_NAME => PostgreSqlTelemetryAttributes::DB_SYSTEM_POSTGRESQL,
            SemConvAttributes::DB_NAMESPACE => $this->connectionParameters->database(),
        ];

        if ($this->queryAttrs->operation !== null) {
            $attributes[SemConvAttributes::DB_OPERATION_NAME] = $this->queryAttrs->operation;
        }

        $this->returnedRows->record($this->rowsIterated, $attributes);
    }
}
