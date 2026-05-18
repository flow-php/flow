<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\Implementation;

use DateTimeImmutable;
use Flow\ETL\Cache;
use Flow\ETL\Cache\CacheIndex;
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\Telemetry\Meter\Instrument\Counter;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\Tracer;
use Throwable;

final readonly class TraceableCache implements Cache
{
    private Counter $hitCounter;

    private Counter $missCounter;

    private Tracer $tracer;

    public function __construct(
        private Cache $cache,
        Telemetry $telemetry,
        private string $dataframeName = 'flow_dataframe',
    ) {
        $this->tracer = $telemetry->tracer('flow_php_dataframe', PackageVersion::get('flow-php/etl'));
        $meter = $telemetry->meter('flow_php_dataframe', PackageVersion::get('flow-php/etl'));
        $this->hitCounter = $meter->createCounter('cache_hits', 'operations', 'Number of cache hits');
        $this->missCounter = $meter->createCounter('cache_misses', 'operations', 'Number of cache misses');
    }

    public function clear(): void
    {
        $span = $this->tracer->span('Cache Clear', SpanKind::CLIENT, [
            'cache.operation' => 'clear',
        ]);

        try {
            $this->cache->clear();
            $span->setStatus(SpanStatus::ok());
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function delete(string $key): void
    {
        $span = $this->tracer->span("Cache Delete {$key}", SpanKind::CLIENT, [
            'cache.operation' => 'delete',
            'cache.key' => $key,
        ]);

        try {
            $this->cache->delete($key);
            $span->setStatus(SpanStatus::ok());
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function get(string $key): Row|Rows|CacheIndex
    {
        $attributes = ['dataframe.name' => $this->dataframeName];

        try {
            $result = $this->cache->get($key);
            $this->hitCounter->add(1, $attributes);

            return $result;
        } catch (KeyNotInCacheException $exception) {
            $this->missCounter->add(1, $attributes);

            throw $exception;
        }
    }

    public function has(string $key): bool
    {
        $attributes = ['dataframe.name' => $this->dataframeName];
        $exists = $this->cache->has($key);

        if ($exists) {
            $this->hitCounter->add(1, $attributes);
        } else {
            $this->missCounter->add(1, $attributes);
        }

        return $exists;
    }

    public function set(string $key, Row|Rows|CacheIndex $value): void
    {
        $valueType = match (true) {
            $value instanceof Row => 'Row',
            $value instanceof Rows => 'Rows',
            $value instanceof CacheIndex => 'CacheIndex',
        };

        $span = $this->tracer->span("Cache Set {$key}", SpanKind::CLIENT, [
            'cache.operation' => 'set',
            'cache.key' => $key,
            'cache.value_type' => $valueType,
        ]);

        try {
            $this->cache->set($key, $value);
            $span->setStatus(SpanStatus::ok());
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }
}
