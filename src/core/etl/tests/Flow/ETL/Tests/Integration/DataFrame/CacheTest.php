<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Cache\CacheIndex;
use Flow\ETL\Cache\Implementation\FilesystemCache;
use Flow\ETL\Cache\Implementation\InMemoryCache;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\Double\SpySerializer;
use Flow\ETL\Tests\FlowIntegrationTestCase;
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
use Flow\Telemetry\Tracer\TracerProvider;
use Generator;

use function array_filter;
use function array_map;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_cache;
use function Flow\ETL\DSL\telemetry_options;
use function Flow\Filesystem\DSL\path;
use function range;

final class CacheTest extends FlowIntegrationTestCase
{
    public function test_cache(): void
    {
        $spyExtractor = new class(20) implements Extractor {
            public function withSchema(Schema $schema): static
            {
                return $this;
            }

            public function schema(): Schema
            {
                return $this->extractor->schema();
            }

            public int $extractions = 0;

            private readonly Extractor $extractor;

            public function __construct(int $rowsets)
            {
                $this->extractor = new FakeExtractor($rowsets);
            }

            public function extract(FlowContext $context): Generator
            {
                $this->extractions++;

                return $this->extractor->extract($context);
            }
        };

        $cache = new InMemoryCache();

        df(config_builder()->cache($cache))
            ->read(from_cache('test_etl_cache', $spyExtractor))
            ->cache('test_etl_cache')
            ->run();

        static::assertEquals(1, $spyExtractor->extractions);
        static::assertInstanceOf(Rows::class, $cache->get('test_etl_cache'));

        df(config_builder()->cache($cache))->read(from_cache('test_etl_cache', $spyExtractor, clear: true))->run();

        static::assertEquals(1, $spyExtractor->extractions);
        static::assertFalse($cache->has('test_etl_cache'));
    }

    public function test_cache_with_previously_set_batch_size(): void
    {
        $cache = new InMemoryCache();

        df(config_builder()->cache($cache))
            ->read(from_array(array_map(static fn(int $i) => ['id' => $i], range(1, 100))))
            ->batchSize(20)
            ->cache('test')
            ->run();

        $indexRows = $cache->get('test');

        static::assertInstanceOf(Rows::class, $indexRows);

        $cacheIndex = CacheIndex::fromRows('test', $indexRows);

        static::assertCount(5, $cacheIndex->values());

        foreach ($cacheIndex->values() as $cacheRowsKey) {
            $rows = $cache->get($cacheRowsKey);
            static::assertInstanceOf(Rows::class, $rows);
            static::assertCount(20, $rows);
        }
    }

    public function test_cache_end_to_end_with_custom_serializer(): void
    {
        $input = array_map(static fn(int $i) => ['id' => $i], range(1, 25));

        $defaultCache = new FilesystemCache($this->fs(), path(__DIR__ . '/var/cache-mode-default'));
        $spy = new SpySerializer();
        $customCache = new FilesystemCache($this->fs(), path(__DIR__ . '/var/cache-mode-custom'), $spy);

        df(config_builder()->cache($defaultCache))->read(from_array($input))->batchSize(10)->cache('parity')->run();
        df(config_builder()->cache($customCache))->read(from_array($input))->batchSize(10)->cache('parity')->run();

        $defaultRows = df(config_builder()->cache($defaultCache))->read(from_cache('parity'))->fetch();
        $customRows = df(config_builder()->cache($customCache))->read(from_cache('parity'))->fetch();

        static::assertSame($input, $defaultRows->toArray());
        static::assertSame($input, $customRows->toArray());
        static::assertNotEmpty($spy->serialized);
        static::assertNotEmpty($spy->unserialized);

        $defaultCache->clear();
        $customCache->clear();
    }

    public function test_cache_with_telemetry_collects_spans_and_metrics(): void
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();

        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'test-service']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        df(config_builder()->withTelemetry($telemetry, telemetry_options(trace_cache: true)))
            ->read(from_array([
                ['id' => 1],
                ['id' => 2],
            ]))
            ->cache('telemetry_test')
            ->run();

        $telemetry->flush();

        $spans = $spanProcessor->endedSpans();
        $setSpans = array_filter($spans, static fn($span) => $span->name() === 'cache.set');

        static::assertNotEmpty($setSpans, 'Expected cache.set spans to be recorded');

        $metricProcessor->metricsWithName('flow.cache.hits');
        $missMetrics = $metricProcessor->metricsWithName('flow.cache.misses');

        static::assertNotEmpty($missMetrics, 'Expected cache miss metrics to be recorded (from has() checks)');
    }

    public function test_cache_without_previously_set_batch_size(): void
    {
        $cache = new InMemoryCache();

        df(config_builder()->cache($cache))
            ->read(from_array(array_map(static fn(int $i) => ['id' => $i], range(1, 100))))
            ->cache('test')
            ->run();

        $indexRows = $cache->get('test');

        static::assertInstanceOf(Rows::class, $indexRows);

        $cacheIndex = CacheIndex::fromRows('test', $indexRows);

        static::assertCount(100, $cacheIndex->values());

        foreach ($cacheIndex->values() as $cacheRowsKey) {
            $rows = $cache->get($cacheRowsKey);
            static::assertInstanceOf(Rows::class, $rows);
            static::assertCount(1, $rows);
        }
    }
}
