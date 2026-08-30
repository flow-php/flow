<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Cache\Implementation;

use Flow\ETL\Cache;
use Flow\ETL\Cache\Implementation\InMemoryCache;
use Flow\ETL\Cache\Implementation\TraceableCache;
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\TracerProvider;
use PHPUnit\Framework\Attributes\CoversClass;
use RuntimeException;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

#[CoversClass(TraceableCache::class)]
final class TraceableCacheTest extends FlowTestCase
{
    private MemoryMetricProcessor $metricProcessor;

    private MemorySpanProcessor $spanProcessor;

    private Telemetry $telemetry;

    protected function setUp(): void
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();

        $this->spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $this->metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());

        $this->telemetry = new Telemetry(
            Resource::create(['service.name' => 'test-service']),
            new TracerProvider($this->spanProcessor, $clock, $contextStorage),
            new MeterProvider($this->metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );
    }

    public function test_clear_creates_span_with_correct_name_and_attributes(): void
    {
        $innerCache = new InMemoryCache();
        $cache = new TraceableCache($innerCache, $this->telemetry);

        $cache->clear();

        $this->telemetry->flush();
        $spans = $this->spanProcessor->endedSpans();

        static::assertCount(1, $spans);

        $span = $spans[0];
        static::assertSame('cache.clear', $span->name());
        static::assertSame(SpanKind::CLIENT, $span->kind());

        $attributes = $span->attributes();
        static::assertSame('clear', $attributes['cache.operation']);
    }

    public function test_delete_creates_span_with_correct_name_and_attributes(): void
    {
        $innerCache = new InMemoryCache();
        $cache = new TraceableCache($innerCache, $this->telemetry);

        $cache->delete('test-key');

        $this->telemetry->flush();
        $spans = $this->spanProcessor->endedSpans();

        static::assertCount(1, $spans);

        $span = $spans[0];
        static::assertSame('cache.delete', $span->name());
        static::assertSame(SpanKind::CLIENT, $span->kind());

        $attributes = $span->attributes();
        static::assertSame('delete', $attributes['cache.operation']);
        static::assertSame('test-key', $attributes['cache.key']);
    }

    public function test_get_increments_hit_counter_on_existing_key(): void
    {
        $innerCache = new InMemoryCache();
        $cache = new TraceableCache($innerCache, $this->telemetry, 'test_dataframe');

        $innerCache->set('existing-key', rows(schema(), row([])));
        $cache->get('existing-key');

        $this->telemetry->flush();
        $hitMetrics = $this->metricProcessor->metricsWithName('flow.cache.hits');
        $missMetrics = $this->metricProcessor->metricsWithName('flow.cache.misses');

        static::assertCount(1, $hitMetrics);
        static::assertCount(0, $missMetrics);
        static::assertSame(1, $hitMetrics[0]->value);
        static::assertSame('{operation}', $hitMetrics[0]->unit);
        static::assertSame('test_dataframe', $hitMetrics[0]->attributes->get('flow.etl.dataframe.name'));
    }

    public function test_get_increments_miss_counter_and_throws_on_non_existing_key(): void
    {
        $innerCache = new InMemoryCache();
        $cache = new TraceableCache($innerCache, $this->telemetry, 'test_dataframe');

        $this->expectException(KeyNotInCacheException::class);

        try {
            $cache->get('non-existing-key');
        } finally {
            $this->telemetry->flush();
            $hitMetrics = $this->metricProcessor->metricsWithName('flow.cache.hits');
            $missMetrics = $this->metricProcessor->metricsWithName('flow.cache.misses');

            static::assertCount(0, $hitMetrics);
            static::assertCount(1, $missMetrics);
            static::assertSame(1, $missMetrics[0]->value);
            static::assertSame('test_dataframe', $missMetrics[0]->attributes->get('flow.etl.dataframe.name'));
        }
    }

    public function test_has_increments_hit_counter_when_key_exists(): void
    {
        $innerCache = new InMemoryCache();
        $cache = new TraceableCache($innerCache, $this->telemetry, 'test_dataframe');

        $innerCache->set('existing-key', rows(schema(), row([])));
        $exists = $cache->has('existing-key');

        static::assertTrue($exists);

        $this->telemetry->flush();
        $hitMetrics = $this->metricProcessor->metricsWithName('flow.cache.hits');
        $missMetrics = $this->metricProcessor->metricsWithName('flow.cache.misses');

        static::assertCount(1, $hitMetrics);
        static::assertCount(0, $missMetrics);
        static::assertSame(1, $hitMetrics[0]->value);
        static::assertSame('{operation}', $hitMetrics[0]->unit);
        static::assertSame('test_dataframe', $hitMetrics[0]->attributes->get('flow.etl.dataframe.name'));
    }

    public function test_has_increments_miss_counter_when_key_does_not_exist(): void
    {
        $innerCache = new InMemoryCache();
        $cache = new TraceableCache($innerCache, $this->telemetry, 'test_dataframe');

        $exists = $cache->has('non-existing-key');

        static::assertFalse($exists);

        $this->telemetry->flush();
        $hitMetrics = $this->metricProcessor->metricsWithName('flow.cache.hits');
        $missMetrics = $this->metricProcessor->metricsWithName('flow.cache.misses');

        static::assertCount(0, $hitMetrics);
        static::assertCount(1, $missMetrics);
        static::assertSame(1, $missMetrics[0]->value);
        static::assertSame('test_dataframe', $missMetrics[0]->attributes->get('flow.etl.dataframe.name'));
    }

    public function test_schema_increments_hit_counter_on_existing_key(): void
    {
        $innerCache = new InMemoryCache();
        $cache = new TraceableCache($innerCache, $this->telemetry, 'test_dataframe');

        $innerCache->set('existing-key', $rows = rows(schema(int_schema('id')), row(['id' => 1])));

        static::assertEquals($rows->schema(), $cache->schema('existing-key'));

        $this->telemetry->flush();
        $hitMetrics = $this->metricProcessor->metricsWithName('flow.cache.hits');

        static::assertCount(1, $hitMetrics);
        static::assertCount(0, $this->metricProcessor->metricsWithName('flow.cache.misses'));
        static::assertSame(1, $hitMetrics[0]->value);
        static::assertSame('test_dataframe', $hitMetrics[0]->attributes->get('flow.etl.dataframe.name'));
    }

    public function test_schema_increments_miss_counter_and_throws_on_non_existing_key(): void
    {
        $cache = new TraceableCache(new InMemoryCache(), $this->telemetry, 'test_dataframe');

        $this->expectException(KeyNotInCacheException::class);

        try {
            $cache->schema('non-existing-key');
        } finally {
            $this->telemetry->flush();
            $missMetrics = $this->metricProcessor->metricsWithName('flow.cache.misses');

            static::assertCount(0, $this->metricProcessor->metricsWithName('flow.cache.hits'));
            static::assertCount(1, $missMetrics);
            static::assertSame(1, $missMetrics[0]->value);
            static::assertSame('test_dataframe', $missMetrics[0]->attributes->get('flow.etl.dataframe.name'));
        }
    }

    public function test_set_creates_span_with_rows_value_type(): void
    {
        $innerCache = new InMemoryCache();
        $cache = new TraceableCache($innerCache, $this->telemetry);

        $cache->set('test-key', rows(schema()));

        $this->telemetry->flush();
        $spans = $this->spanProcessor->endedSpans();

        static::assertCount(1, $spans);

        $span = $spans[0];
        static::assertSame('cache.set', $span->name());
        static::assertSame(SpanKind::CLIENT, $span->kind());

        $attributes = $span->attributes();
        static::assertSame('set', $attributes['cache.operation']);
        static::assertSame('test-key', $attributes['cache.key']);
        static::assertSame('Rows', $attributes['cache.value_type']);
    }

    public function test_clear_records_exception_on_error(): void
    {
        $innerCache = $this->createStub(Cache::class);
        $innerCache->method('clear')->willThrowException(new RuntimeException('Test error'));

        $cache = new TraceableCache($innerCache, $this->telemetry);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Test error');

        try {
            $cache->clear();
        } finally {
            $this->telemetry->flush();
            $spans = $this->spanProcessor->endedSpans();

            static::assertCount(1, $spans);
            static::assertTrue($spans[0]->status()?->isError());
            static::assertSame(RuntimeException::class, $spans[0]->attributes()['error.type']);
        }
    }

    public function test_delete_records_exception_on_error(): void
    {
        $innerCache = $this->createStub(Cache::class);
        $innerCache->method('delete')->willThrowException(new RuntimeException('Test error'));

        $cache = new TraceableCache($innerCache, $this->telemetry);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Test error');

        try {
            $cache->delete('test-key');
        } finally {
            $this->telemetry->flush();
            $spans = $this->spanProcessor->endedSpans();

            static::assertCount(1, $spans);
            static::assertTrue($spans[0]->status()?->isError());
            static::assertSame(RuntimeException::class, $spans[0]->attributes()['error.type']);
        }
    }

    public function test_set_records_exception_on_error(): void
    {
        $innerCache = $this->createStub(Cache::class);
        $innerCache->method('set')->willThrowException(new RuntimeException('Test error'));

        $cache = new TraceableCache($innerCache, $this->telemetry);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Test error');

        try {
            $cache->set('test-key', rows(schema(), row([])));
        } finally {
            $this->telemetry->flush();
            $spans = $this->spanProcessor->endedSpans();

            static::assertCount(1, $spans);

            $span = $spans[0];
            $status = $span->status();
            static::assertNotNull($status);
            static::assertTrue($status->isError());
            static::assertSame('Test error', $status->description);
            static::assertSame(RuntimeException::class, $span->attributes()['error.type']);
            static::assertCount(1, $span->events());
        }
    }
}
