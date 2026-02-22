<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Cache\Implementation;

use function Flow\ETL\DSL\{row, rows};
use Flow\ETL\Cache;
use Flow\ETL\Cache\CacheIndex;
use Flow\ETL\Cache\Implementation\{InMemoryCache, TraceableCache};
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\{MemoryLogProcessor, MemoryMetricProcessor, MemorySpanProcessor};
use Flow\Telemetry\Provider\Void\{VoidLogExporter, VoidMetricExporter, VoidSpanExporter};
use Flow\Telemetry\{Resource, Telemetry};
use Flow\Telemetry\Tracer\{SpanKind, TracerProvider};
use PHPUnit\Framework\Attributes\CoversClass;

#[CoversClass(TraceableCache::class)]
final class TraceableCacheTest extends FlowTestCase
{
    private MemoryMetricProcessor $metricProcessor;

    private MemorySpanProcessor $spanProcessor;

    private Telemetry $telemetry;

    protected function setUp() : void
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();

        $this->spanProcessor = new MemorySpanProcessor(new VoidSpanExporter());
        $this->metricProcessor = new MemoryMetricProcessor(new VoidMetricExporter());
        $logProcessor = new MemoryLogProcessor(new VoidLogExporter());

        $this->telemetry = new Telemetry(
            Resource::create(['service.name' => 'test-service']),
            new TracerProvider($this->spanProcessor, $clock, $contextStorage),
            new MeterProvider($this->metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );
    }

    public function test_clear_creates_span_with_correct_name_and_attributes() : void
    {
        $innerCache = new InMemoryCache();
        $cache = new TraceableCache($innerCache, $this->telemetry);

        $cache->clear();

        $this->telemetry->flush();
        $spans = $this->spanProcessor->endedSpans();

        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('Cache Clear', $span->name());
        self::assertSame(SpanKind::CLIENT, $span->kind());

        $attributes = $span->attributes();
        self::assertSame('clear', $attributes['cache.operation']);
    }

    public function test_delete_creates_span_with_correct_name_and_attributes() : void
    {
        $innerCache = new InMemoryCache();
        $cache = new TraceableCache($innerCache, $this->telemetry);

        $cache->delete('test-key');

        $this->telemetry->flush();
        $spans = $this->spanProcessor->endedSpans();

        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('Cache Delete test-key', $span->name());
        self::assertSame(SpanKind::CLIENT, $span->kind());

        $attributes = $span->attributes();
        self::assertSame('delete', $attributes['cache.operation']);
        self::assertSame('test-key', $attributes['cache.key']);
    }

    public function test_get_increments_hit_counter_on_existing_key() : void
    {
        $innerCache = new InMemoryCache();
        $cache = new TraceableCache($innerCache, $this->telemetry, 'test_dataframe');

        $innerCache->set('existing-key', row());
        $cache->get('existing-key');

        $this->telemetry->flush();
        $hitMetrics = $this->metricProcessor->metricsWithName('cache_hits');
        $missMetrics = $this->metricProcessor->metricsWithName('cache_misses');

        self::assertCount(1, $hitMetrics);
        self::assertCount(0, $missMetrics);
        self::assertSame(1, $hitMetrics[0]->value);
        self::assertSame('test_dataframe', $hitMetrics[0]->attributes->get('dataframe.name'));
    }

    public function test_get_increments_miss_counter_and_throws_on_non_existing_key() : void
    {
        $innerCache = new InMemoryCache();
        $cache = new TraceableCache($innerCache, $this->telemetry, 'test_dataframe');

        $this->expectException(KeyNotInCacheException::class);

        try {
            $cache->get('non-existing-key');
        } finally {
            $this->telemetry->flush();
            $hitMetrics = $this->metricProcessor->metricsWithName('cache_hits');
            $missMetrics = $this->metricProcessor->metricsWithName('cache_misses');

            self::assertCount(0, $hitMetrics);
            self::assertCount(1, $missMetrics);
            self::assertSame(1, $missMetrics[0]->value);
            self::assertSame('test_dataframe', $missMetrics[0]->attributes->get('dataframe.name'));
        }
    }

    public function test_has_increments_hit_counter_when_key_exists() : void
    {
        $innerCache = new InMemoryCache();
        $cache = new TraceableCache($innerCache, $this->telemetry, 'test_dataframe');

        $innerCache->set('existing-key', row());
        $exists = $cache->has('existing-key');

        self::assertTrue($exists);

        $this->telemetry->flush();
        $hitMetrics = $this->metricProcessor->metricsWithName('cache_hits');
        $missMetrics = $this->metricProcessor->metricsWithName('cache_misses');

        self::assertCount(1, $hitMetrics);
        self::assertCount(0, $missMetrics);
        self::assertSame(1, $hitMetrics[0]->value);
        self::assertSame('test_dataframe', $hitMetrics[0]->attributes->get('dataframe.name'));
    }

    public function test_has_increments_miss_counter_when_key_does_not_exist() : void
    {
        $innerCache = new InMemoryCache();
        $cache = new TraceableCache($innerCache, $this->telemetry, 'test_dataframe');

        $exists = $cache->has('non-existing-key');

        self::assertFalse($exists);

        $this->telemetry->flush();
        $hitMetrics = $this->metricProcessor->metricsWithName('cache_hits');
        $missMetrics = $this->metricProcessor->metricsWithName('cache_misses');

        self::assertCount(0, $hitMetrics);
        self::assertCount(1, $missMetrics);
        self::assertSame(1, $missMetrics[0]->value);
        self::assertSame('test_dataframe', $missMetrics[0]->attributes->get('dataframe.name'));
    }

    public function test_set_creates_span_with_cache_index_value_type() : void
    {
        $innerCache = new InMemoryCache();
        $cache = new TraceableCache($innerCache, $this->telemetry);

        $cache->set('test-key', new CacheIndex('test-id'));

        $this->telemetry->flush();
        $spans = $this->spanProcessor->endedSpans();

        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('Cache Set test-key', $span->name());
        self::assertSame(SpanKind::CLIENT, $span->kind());

        $attributes = $span->attributes();
        self::assertSame('set', $attributes['cache.operation']);
        self::assertSame('test-key', $attributes['cache.key']);
        self::assertSame('CacheIndex', $attributes['cache.value_type']);
    }

    public function test_set_creates_span_with_correct_name_and_attributes_for_row() : void
    {
        $innerCache = new InMemoryCache();
        $cache = new TraceableCache($innerCache, $this->telemetry);

        $cache->set('test-key', row());

        $this->telemetry->flush();
        $spans = $this->spanProcessor->endedSpans();

        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('Cache Set test-key', $span->name());
        self::assertSame(SpanKind::CLIENT, $span->kind());

        $attributes = $span->attributes();
        self::assertSame('set', $attributes['cache.operation']);
        self::assertSame('test-key', $attributes['cache.key']);
        self::assertSame('Row', $attributes['cache.value_type']);
    }

    public function test_set_creates_span_with_rows_value_type() : void
    {
        $innerCache = new InMemoryCache();
        $cache = new TraceableCache($innerCache, $this->telemetry);

        $cache->set('test-key', rows());

        $this->telemetry->flush();
        $spans = $this->spanProcessor->endedSpans();

        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('Cache Set test-key', $span->name());
        self::assertSame(SpanKind::CLIENT, $span->kind());

        $attributes = $span->attributes();
        self::assertSame('set', $attributes['cache.operation']);
        self::assertSame('test-key', $attributes['cache.key']);
        self::assertSame('Rows', $attributes['cache.value_type']);
    }

    public function test_set_records_exception_on_error() : void
    {
        $innerCache = $this->createMock(Cache::class);
        $innerCache->method('set')->willThrowException(new \RuntimeException('Test error'));

        $cache = new TraceableCache($innerCache, $this->telemetry);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Test error');

        try {
            $cache->set('test-key', row());
        } finally {
            $this->telemetry->flush();
            $spans = $this->spanProcessor->endedSpans();

            self::assertCount(1, $spans);

            $span = $spans[0];
            self::assertNotNull($span->status());
            self::assertTrue($span->status()->isError());
            self::assertSame('Test error', $span->status()->description);
            self::assertCount(1, $span->events());
        }
    }
}
