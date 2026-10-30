<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Storage\FilesystemRowsStorage;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Storage\PsrRowsStorage;
use Flow\ETL\Tests\Double\ArrayCache;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\Double\SpySerializer;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
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
use PHPUnit\Framework\Attributes\DataProvider;

use function array_filter;
use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_cache;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\telemetry_options;
use function iterator_to_array;
use function range;

final class CacheTest extends FlowIntegrationTestCase
{
    /**
     * @return Generator<string, array{bool}>
     */
    public static function drifts(): Generator
    {
        yield 'a column is added' => [true];
        yield 'a column turns nullable' => [false];
    }

    #[DataProvider('drifts')]
    public function test_a_stream_whose_schema_drifts_round_trips(bool $added): void
    {
        df()
            ->read(from_rows(
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
                array_to_rows(
                    [
                        $added ? ['id' => 2, 'name' => 'two'] : ['id' => null],
                    ],
                    $added
                        ? schema(int_schema('id'), str_schema('name', nullable: true))
                        : schema(int_schema('id', nullable: true)),
                ),
            ))
            ->cache('drift')
            ->run();

        static::assertSame(
            $added ? [['id' => 1, 'name' => null], ['id' => 2, 'name' => 'two']] : [['id' => 1], ['id' => null]],
            df()->read(from_cache('drift'))->fetch()->toArray(),
        );
    }

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

            public function extract(Backend $backend, Criteria $criteria): Generator
            {
                $this->extractions++;

                return $this->extractor->extract($backend, new Criteria());
            }

            public function statistics(): Statistics
            {
                return new Statistics();
            }

            public function isRepeatable(): bool
            {
                return false;
            }
        };

        $cache = new MemoryRowsStorage();

        df(config_builder()->cache($cache))
            ->read(from_cache('test_etl_cache', $spyExtractor))
            ->cache('test_etl_cache')
            ->run();

        static::assertEquals(1, $spyExtractor->extractions);
        static::assertTrue($cache->has('test_etl_cache'));

        df(config_builder()->cache($cache))->read(from_cache('test_etl_cache', $spyExtractor, clear: true))->run();

        static::assertEquals(1, $spyExtractor->extractions);
        static::assertFalse($cache->has('test_etl_cache'));
    }

    public function test_cache_with_previously_set_batch_size(): void
    {
        $cache = new MemoryRowsStorage();

        df(config_builder()->cache($cache))
            ->read(from_array(array_map(static fn(int $i) => ['id' => $i], range(1, 100))))
            ->batchSize(20)
            ->cache('test')
            ->run();

        static::assertSame(
            [20, 20, 20, 20, 20],
            array_map(static fn(Rows $rows): int => $rows->count(), iterator_to_array($cache->get('test'), false)),
        );
    }

    public function test_cache_end_to_end_on_the_filesystem_and_through_a_custom_serializer(): void
    {
        $input = array_map(static fn(int $i) => ['id' => $i], range(1, 25));

        $defaultCache = new FilesystemRowsStorage(
            $this->fs(),
            $this->cacheDir->suffix('/cache-mode-default'),
            new AdaptiveBackend(),
        );
        $spy = new SpySerializer();
        $customCache = new PsrRowsStorage(new ArrayCache(), $spy);

        df(config_builder()->cache($defaultCache))->read(from_array($input))->batchSize(10)->cache('parity')->run();
        df(config_builder()->cache($customCache))->read(from_array($input))->batchSize(10)->cache('parity')->run();

        $defaultRows = df(config_builder()->cache($defaultCache))->read(from_cache('parity'))->fetch();
        $customRows = df(config_builder()->cache($customCache))->read(from_cache('parity'))->fetch();

        static::assertSame($input, $defaultRows->toArray());
        static::assertSame($input, $customRows->toArray());
        static::assertNotEmpty($spy->serialized);
        static::assertNotEmpty($spy->unserialized);
    }

    public function test_cache_with_telemetry_collects_spans_and_metrics(): void
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();

        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'test-service']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider(new MemoryLogProcessor(new VoidExporter()), $clock, $contextStorage),
        );

        df(config_builder()->withTelemetry($telemetry, telemetry_options(trace_cache: true)))
            ->read(from_array([
                ['id' => 1],
                ['id' => 2],
            ]))
            ->cache('telemetry_test')
            ->run();

        $telemetry->flush();

        static::assertNotEmpty(
            array_filter($spanProcessor->endedSpans(), static fn($span) => $span->name() === 'cache.set'),
            'Expected cache.set spans to be recorded',
        );

        $metricProcessor->metricsWithName('flow.cache.hits');

        static::assertNotEmpty(
            $metricProcessor->metricsWithName('flow.cache.misses'),
            'Expected cache miss metrics to be recorded (from has() checks)',
        );
    }

    public function test_cache_without_previously_set_batch_size(): void
    {
        $cache = new MemoryRowsStorage();

        df(config_builder()->cache($cache))
            ->read(from_array(array_map(static fn(int $i) => ['id' => $i], range(1, 100))))
            ->cache('test')
            ->run();

        // no batch size was set, so the cache holds the one batch the extractor yielded
        static::assertSame(
            [100],
            array_map(static fn(Rows $rows): int => $rows->count(), iterator_to_array($cache->get('test'), false)),
        );
    }
}
