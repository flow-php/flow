<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL;

use DateTimeImmutable;
use Doctrine\DBAL\Driver\Exception as DriverException;
use Flow\Telemetry\Meter\Instrument\Histogram;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\SemConvAttributes;
use Flow\Telemetry\SemConvMetrics;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\Tracer;
use Throwable;

use function array_key_exists;
use function hrtime;
use function mb_strlen;
use function mb_substr;

/**
 * Builds span attributes and records metrics for Doctrine DBAL query operations, shared between
 * the connection (exec/query/prepare) and the statement (execute) instrumentation.
 */
final class QueryTracer
{
    private ?Histogram $operationDuration = null;

    private ?Histogram $returnedRows = null;

    private readonly SqlAttributesExtractor $extractor;

    private readonly ParameterFormatter $parameterFormatter;

    /**
     * @param array<string, int|string> $baseAttributes db.system.name, db.namespace, server.address, server.port
     */
    public function __construct(
        private readonly Telemetry $telemetry,
        private readonly array $baseAttributes,
        private readonly int $maxSqlLength,
        bool $collectMetrics,
        private readonly bool $includeParameters,
        private readonly int $maxParameters,
        private readonly int $maxParameterLength,
    ) {
        $this->extractor = new SqlAttributesExtractor();
        $this->parameterFormatter = new ParameterFormatter();

        if ($collectMetrics) {
            $meter = $this->telemetry->meter('flow.symfony.dbal', PackageVersion::get('doctrine/dbal'));
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
    }

    public function tracer(): Tracer
    {
        return $this->telemetry->tracer('flow.symfony.dbal', PackageVersion::get('doctrine/dbal'));
    }

    public function extract(string $sql): SqlAttributes
    {
        return $this->extractor->extract($sql);
    }

    public function spanName(SqlAttributes $sql): string
    {
        if ($sql->operation !== null && $sql->collection !== null) {
            return $sql->operation . ' ' . $sql->collection;
        }

        return $sql->operation ?? 'query';
    }

    /**
     * @return array<string, int|string>
     */
    public function queryAttributes(string $sql, SqlAttributes $attributes): array
    {
        $result = $this->baseAttributes;

        if ($attributes->operation !== null) {
            $result[SemConvAttributes::DB_OPERATION_NAME] = $attributes->operation;
        }

        if ($attributes->collection !== null) {
            $result[SemConvAttributes::DB_COLLECTION_NAME] = $attributes->collection;
        }

        $result[SemConvAttributes::DB_QUERY_TEXT] = $this->truncateSql($sql);

        return $result;
    }

    /**
     * @param array<int|string, mixed> $parameters
     *
     * @return array<string, string>
     */
    public function parameterAttributes(array $parameters): array
    {
        if (!$this->includeParameters || $parameters === []) {
            return [];
        }

        return $this->parameterFormatter->format($parameters, $this->maxParameters, $this->maxParameterLength);
    }

    public function recordError(Span $span, Throwable $exception): void
    {
        $span->recordException($exception, new DateTimeImmutable());
        $span->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class);

        if ($exception instanceof DriverException) {
            $sqlState = $exception->getSQLState();

            if ($sqlState !== null) {
                $span->setAttribute(SemConvAttributes::DB_RESPONSE_STATUS_CODE, $sqlState);
            }
        }

        $span->setStatus(SpanStatus::error($exception->getMessage()));
    }

    public function recordQueryMetrics(int|float|false $startTime, ?int $rowCount, SqlAttributes $attributes): void
    {
        $metricAttributes = $this->metricAttributes($attributes);
        $duration = $this->elapsed($startTime);
        $operationDuration = $this->operationDuration;

        if ($operationDuration !== null && $duration !== null) {
            $operationDuration->record($duration, $metricAttributes);
        }

        $returnedRows = $this->returnedRows;

        if ($rowCount !== null && $returnedRows !== null) {
            $returnedRows->record($rowCount, $metricAttributes);
        }
    }

    /**
     * @param array<string, int|string> $attributes
     */
    public function recordDuration(int|float|false $startTime, array $attributes): void
    {
        $duration = $this->elapsed($startTime);
        $operationDuration = $this->operationDuration;

        if ($operationDuration !== null && $duration !== null) {
            $operationDuration->record($duration, $attributes);
        }
    }

    /**
     * Low-cardinality subset used for metrics; query text and parameter values are deliberately excluded.
     *
     * @return array<string, int|string>
     */
    private function metricAttributes(SqlAttributes $attributes): array
    {
        $result = [];

        foreach ([SemConvAttributes::DB_SYSTEM_NAME, SemConvAttributes::DB_NAMESPACE] as $key) {
            if (array_key_exists($key, $this->baseAttributes)) {
                $result[$key] = $this->baseAttributes[$key];
            }
        }

        if ($attributes->operation !== null) {
            $result[SemConvAttributes::DB_OPERATION_NAME] = $attributes->operation;
        }

        if ($attributes->collection !== null) {
            $result[SemConvAttributes::DB_COLLECTION_NAME] = $attributes->collection;
        }

        return $result;
    }

    private function elapsed(int|float|false $startTime): ?float
    {
        if ($startTime === false) {
            return null;
        }

        $now = hrtime(true);

        if ($now === false) {
            return null;
        }

        return ($now - $startTime) / 1_000_000_000;
    }

    private function truncateSql(string $sql): string
    {
        if ($this->maxSqlLength <= 0 || mb_strlen($sql) <= $this->maxSqlLength) {
            return $sql;
        }

        return mb_substr($sql, 0, $this->maxSqlLength) . '...';
    }
}
