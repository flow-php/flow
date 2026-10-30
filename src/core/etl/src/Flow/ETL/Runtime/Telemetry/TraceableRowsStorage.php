<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Telemetry;

use DateTimeImmutable;
use Flow\ETL\Foundation\Telemetry\TelemetryAttributes;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Telemetry\CacheAttributes;
use Flow\Telemetry\Meter\Instrument\Counter;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\SemConvAttributes;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\Tracer;
use Generator;
use Throwable;

final readonly class TraceableRowsStorage implements RowsStorage
{
    private Counter $hitCounter;

    private Counter $missCounter;

    private Tracer $tracer;

    public function __construct(
        private RowsStorage $storage,
        Telemetry $telemetry,
        private string $dataframeName = 'flow_dataframe',
    ) {
        $this->tracer = $telemetry->tracer('flow_php_dataframe', PackageVersion::get('flow-php/etl'));
        $meter = $telemetry->meter('flow_php_dataframe', PackageVersion::get('flow-php/etl'));
        $this->hitCounter = $meter->createCounter('flow.cache.hits', '{operation}', 'Number of cache hits');
        $this->missCounter = $meter->createCounter('flow.cache.misses', '{operation}', 'Number of cache misses');
    }

    public function append(string $key, Rows $rows): void
    {
        $span = $this->tracer->span(CacheAttributes::SPAN_SET, SpanKind::CLIENT, [
            CacheAttributes::CACHE_OPERATION => 'append',
            CacheAttributes::CACHE_KEY => $key,
            CacheAttributes::CACHE_VALUE_TYPE => 'Rows',
        ]);
        $scope = $this->tracer->activate($span);

        try {
            $this->storage->append($key, $rows);
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $scope->detach();
            $this->tracer->complete($span);
        }
    }

    public function close(string $key): void
    {
        $this->storage->close($key);
    }

    public function get(string $key): Generator
    {
        yield from $this->storage->get($key);
    }

    public function has(string $key): bool
    {
        $attributes = [TelemetryAttributes::ATTR_DATAFRAME_NAME => $this->dataframeName];
        $exists = $this->storage->has($key);

        if ($exists) {
            $this->hitCounter->add(1, $attributes);
        } else {
            $this->missCounter->add(1, $attributes);
        }

        return $exists;
    }

    public function remove(string $key): void
    {
        $span = $this->tracer->span(CacheAttributes::SPAN_DELETE, SpanKind::CLIENT, [
            CacheAttributes::CACHE_OPERATION => 'remove',
            CacheAttributes::CACHE_KEY => $key,
        ]);

        try {
            $this->storage->remove($key);
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function rows(string $key): Cardinality
    {
        return $this->storage->rows($key);
    }

    public function schema(string $key): ?Schema
    {
        return $this->storage->schema($key);
    }
}
