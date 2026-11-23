<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Executor;

use DateTimeImmutable;
use Flow\ETL\API\Config\ConfigBuilder;
use Flow\ETL\Foundation\Clock\FakeClock;
use Flow\ETL\Foundation\Clock\HighResolutionTime;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Runtime\Executor\StatisticsCollector;
use Flow\ETL\Runtime\Executor\WholeOutputHasher;
use Flow\ETL\Runtime\Metrics\Hash;
use Flow\ETL\Runtime\Metrics\PartitionHash;
use Flow\ETL\Runtime\Metrics\Report;
use Flow\ETL\Runtime\Metrics\SourceStatistics;
use Flow\ETL\Runtime\Metrics\Statistics\Columns;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\Double\StubLauncher;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Memory\Unit;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Partitions;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\Severity;

use function array_map;
use function Flow\ETL\DSL\analyze;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;
use function memory_get_peak_usage;

final class StatisticsCollectorTest extends FlowTestCase
{
    public function test_capture_collects_column_statistics_when_enabled(): void
    {
        $collector = new StatisticsCollector(analyze()->withColumnStatistics(), flow_context());

        $collector->capture(array_to_rows(
            [['id' => 1, 'name' => 'Alice'], ['id' => 2, 'name' => 'Bob']],
            schema(int_schema('id'), str_schema('name')),
        ));

        $report = $collector->report();

        static::assertNotNull($report);
        $columns = $report->statistics()->columns;
        static::assertInstanceOf(Columns::class, $columns);
        static::assertSame(1, $columns->get('id')->min());
        static::assertSame(2, $columns->get('id')->max());
    }

    public function test_capture_collects_schema_when_enabled(): void
    {
        $collector = new StatisticsCollector(analyze()->withSchema(), flow_context());

        $collector->capture(array_to_rows(
            [['id' => 1, 'name' => 'Alice']],
            schema(int_schema('id'), str_schema('name')),
        ));

        $report = $collector->report();

        static::assertNotNull($report);
        $schema = $report->schema();
        static::assertNotNull($schema);
        static::assertSame(2, $schema->count());
        static::assertNotNull($schema->findDefinition('id'));
        static::assertNotNull($schema->findDefinition('name'));
    }

    public function test_capture_increments_row_count_correctly(): void
    {
        $collector = new StatisticsCollector(true, flow_context());

        $collector->capture(array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $collector->capture(array_to_rows([['id' => 3]], schema(int_schema('id'))));

        $report = $collector->report();

        static::assertNotNull($report);
        static::assertSame(3, $report->statistics()->totalRows());
    }

    public function test_capture_is_noop_when_analyze_is_false(): void
    {
        $collector = new StatisticsCollector(false, flow_context());

        $collector->capture(array_to_rows([['id' => 1]], schema(int_schema('id'))));

        static::assertNull($collector->report());
    }

    public function test_column_statistics_is_null_when_not_enabled(): void
    {
        $collector = new StatisticsCollector(analyze(), flow_context());

        $collector->capture(array_to_rows([['id' => 1]], schema(int_schema('id'))));

        $report = $collector->report();

        static::assertNotNull($report);
        static::assertNull($report->statistics()->columns);
    }

    public function test_report_returns_null_when_analyze_is_false(): void
    {
        static::assertNull((new StatisticsCollector(false, flow_context()))->report());
    }

    public function test_report_returns_null_when_analyze_is_null(): void
    {
        static::assertNull((new StatisticsCollector(null, flow_context()))->report());
    }

    public function test_report_returns_report_when_analyze_is_true(): void
    {
        static::assertInstanceOf(Report::class, (new StatisticsCollector(true, flow_context()))->report());
    }

    public function test_report_returns_report_when_using_analyze_instance(): void
    {
        static::assertInstanceOf(Report::class, (new StatisticsCollector(analyze(), flow_context()))->report());
    }

    public function test_report_returns_report_with_correct_execution_time(): void
    {
        $clock = new FakeClock(new DateTimeImmutable('2025-01-01 10:00:00 UTC'));
        $collector = new StatisticsCollector(
            true,
            flow_context(
                (new ConfigBuilder())
                    ->clock($clock)
                    ->build(),
            ),
        );

        $clock->modify('+5 minutes');
        $collector->capture(array_to_rows([['id' => 1]], schema(int_schema('id'))));

        $clock->modify('+5 minutes');
        $report = $collector->report();

        static::assertNotNull($report);
        static::assertEquals(
            new DateTimeImmutable('2025-01-01 10:00:00 UTC'),
            $report->statistics()->executionTime->startedAt,
        );
        static::assertEquals(
            new DateTimeImmutable('2025-01-01 10:10:00 UTC'),
            $report->statistics()->executionTime->finishedAt,
        );
        static::assertSame(600, $report->statistics()->executionTime->inSeconds());
    }

    public function test_report_returns_report_with_high_resolution_time(): void
    {
        $collector = new StatisticsCollector(true, flow_context());

        $collector->capture(array_to_rows([['id' => 1]], schema(int_schema('id'))));

        $report = $collector->report();

        static::assertNotNull($report);
        static::assertInstanceOf(HighResolutionTime::class, $report->statistics()->executionTime->highResolutionTime);
    }

    public function test_report_returns_report_with_memory_consumption(): void
    {
        $collector = new StatisticsCollector(true, flow_context());

        $collector->capture(array_to_rows([['id' => 1]], schema(int_schema('id'))));

        $report = $collector->report();

        static::assertNotNull($report);
        static::assertGreaterThanOrEqual(0, $report->statistics()->heap->initial()->inBytes());
    }

    public function test_sources_are_counted_and_reported_when_enabled(): void
    {
        $collector = new StatisticsCollector(analyze()->withSourceStatistics(), flow_context());
        $extractor = from_array([['id' => 1], ['id' => 2]]);
        $counts = $collector->counts();
        static::assertNotNull($counts);

        iterator_to_array($counts->count(
            $extractor,
            $extractor->extract(flow_context()->backend(), new Criteria()),
            false,
        ));

        static::assertEquals(
            [new SourceStatistics('ArrayExtractor', new Statistics(Cardinality::exact(2)), 2, true)],
            $collector->report()->sources(),
        );
    }

    public function test_sources_are_counted_but_not_reported_when_not_enabled(): void
    {
        $collector = new StatisticsCollector(analyze(), flow_context());
        $extractor = from_array([['id' => 1]]);
        $counts = $collector->counts();
        static::assertNotNull($counts);

        iterator_to_array($counts->count(
            $extractor,
            $extractor->extract(flow_context()->backend(), new Criteria()),
            false,
        ));

        static::assertNull($collector->report()->sources());
    }

    public function test_nothing_is_counted_when_analyze_is_false(): void
    {
        static::assertNull((new StatisticsCollector(false, flow_context()))->counts());
    }

    public function test_a_serial_run_reports_the_parent_peak_and_no_workers(): void
    {
        $processes = (new StatisticsCollector(analyze(), flow_context()))->report()->statistics()->processes;

        static::assertSame([], $processes->workers);
        static::assertGreaterThanOrEqual(memory_get_peak_usage(true), $processes->parent->inBytes());
    }

    public function test_workers_counted_under_analyze_reach_the_report(): void
    {
        $collector = new StatisticsCollector(analyze(), flow_context());
        $collector->counts()?->worker(101, 40_000_000);
        $collector->counts()?->worker(102, 41_000_000);

        static::assertEquals(
            [Unit::fromBytes(40_000_000), Unit::fromBytes(41_000_000)],
            $collector->report()->statistics()->processes->workers,
        );
    }

    public function test_schema_is_null_when_not_enabled(): void
    {
        $collector = new StatisticsCollector(analyze(), flow_context());

        $collector->capture(array_to_rows([['id' => 1]], schema(int_schema('id'))));

        $report = $collector->report();

        static::assertNotNull($report);
        static::assertNull($report->schema());
    }

    public function test_reads_columns(): void
    {
        static::assertTrue((new StatisticsCollector(analyze()->withSchema(), flow_context()))->readsColumns());
        static::assertTrue(
            (new StatisticsCollector(analyze()->withColumnStatistics(), flow_context()))->readsColumns(),
        );
        static::assertFalse(
            (new StatisticsCollector(analyze()->withSourceStatistics(), flow_context()))->readsColumns(),
        );
        static::assertFalse((new StatisticsCollector(false, flow_context()))->readsColumns());
    }

    public function test_reading_columns_under_a_launcher_warns_that_writes_do_not_run_in_parallel(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $config = config_builder()->withTelemetry($telemetry->telemetry)->launcher(new StubLauncher())->build();

        new StatisticsCollector(analyze()->withSchema()->withColumnStatistics(), flow_context($config));

        static::assertSame(
            [
                'analyze()->withSchema()->withColumnStatistics() sends every row to the parent process, so the writes do not '
                    . 'run in parallel. Drop it to keep parallel writes.',
            ],
            array_map(
                static fn(LogEntry $entry): string => $entry->record->body,
                $telemetry->logs->entriesWithSeverity(Severity::WARN),
            ),
        );
    }

    public function test_column_statistics_alone_under_a_launcher_names_only_them(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $config = config_builder()->withTelemetry($telemetry->telemetry)->launcher(new StubLauncher())->build();

        new StatisticsCollector(analyze()->withColumnStatistics(), flow_context($config));

        static::assertStringStartsWith(
            'analyze()->withColumnStatistics() sends',
            $telemetry->logs->entriesWithSeverity(Severity::WARN)[0]->record->body,
        );
    }

    public function test_nothing_is_warned_without_a_launcher_or_without_reading_columns(): void
    {
        $telemetry = new MemoryTelemetryContext();

        new StatisticsCollector(
            analyze()->withSchema(),
            flow_context(config_builder()->withTelemetry($telemetry->telemetry)->build()),
        );
        new StatisticsCollector(
            analyze()->withSourceStatistics(),
            flow_context(config_builder()->withTelemetry($telemetry->telemetry)->launcher(new StubLauncher())->build()),
        );

        static::assertSame([], $telemetry->logs->entriesWithSeverity(Severity::WARN));
    }

    public function test_the_dataset_hash_is_the_multiset_hash_of_every_captured_row(): void
    {
        $collector = new StatisticsCollector(analyze()->withDatasetHash(), flow_context());

        $collector->capture(array_to_rows([['a' => 2]], schema(int_schema('a'))));
        $collector->capture(array_to_rows([], schema(int_schema('a'))));
        $collector->capture(array_to_rows([['a' => 1]], schema(int_schema('a'))));

        static::assertEquals(
            new Hash(2, '721f4650154ebb88ac4ec50a605901df'),
            $collector->report()->datasetHash()?->total,
        );
        static::assertSame([], $collector->report()->datasetHash()?->partitions);
        static::assertFalse($collector->readsColumns());
    }

    public function test_a_dataset_hash_of_no_rows_is_zero(): void
    {
        static::assertEquals(
            new Hash(0, '00000000000000000000000000000000'),
            (new StatisticsCollector(analyze()->withDatasetHash(), flow_context()))->report()->datasetHash()?->total,
        );
    }

    public function test_without_the_dataset_hash_no_accumulator_is_created_and_none_is_reported(): void
    {
        $backend = new SpyBackend();
        $collector = new StatisticsCollector(
            analyze()->withSchema()->withColumnStatistics()->withSourceStatistics(),
            flow_context(config_builder()->backend($backend)->build()),
        );

        $collector->capture(array_to_rows([['a' => 1]], schema(int_schema('a')), $backend));

        static::assertNull($collector->report()->datasetHash());
        static::assertSame(0, $backend->groupedMultisetHashesCreated());
        static::assertFalse(
            (new StatisticsCollector(
                analyze()->withSourceStatistics(),
                flow_context(config_builder()->backend($backend)->build()),
            ))->readsColumns(),
        );
    }

    public function test_the_dataset_hash_takes_its_accumulator_from_the_context_backend(): void
    {
        $backend = new SpyBackend();

        $collector = new StatisticsCollector(
            analyze()->withDatasetHash(),
            flow_context(config_builder()->backend($backend)->build()),
        );
        $collector->capture(array_to_rows([['a' => 1]], schema(int_schema('a')), $backend));

        static::assertSame(1, $backend->groupedMultisetHashesCreated());
    }

    public function test_the_dataset_hash_under_a_launcher_is_not_named_in_the_warning(): void
    {
        $telemetry = new MemoryTelemetryContext();

        new StatisticsCollector(
            analyze()->withSchema()->withDatasetHash(),
            flow_context(config_builder()->withTelemetry($telemetry->telemetry)->launcher(new StubLauncher())->build()),
        );

        static::assertSame(
            'analyze()->withSchema() sends every row to the parent process, so the writes do not '
            . 'run in parallel. Drop it to keep parallel writes.',
            $telemetry->logs->entriesWithSeverity(Severity::WARN)[0]->record->body,
        );
    }

    public function test_the_dataset_hash_alone_under_a_launcher_keeps_the_writes_parallel(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $collector = new StatisticsCollector(
            analyze()->withDatasetHash(),
            flow_context(config_builder()->withTelemetry($telemetry->telemetry)->launcher(new StubLauncher())->build()),
        );

        static::assertFalse($collector->readsColumns());
        static::assertInstanceOf(WholeOutputHasher::class, $collector->counts()?->outputHasher());
        static::assertSame([], $telemetry->logs->entriesWithSeverity(Severity::WARN));
    }

    public function test_a_partitioned_dataset_hash_reports_one_hash_per_partition_and_their_total(): void
    {
        $schema = schema(str_schema('country'), int_schema('id'));
        $whole = new StatisticsCollector(analyze()->withDatasetHash(), flow_context());
        $collector = new StatisticsCollector(analyze()->withDatasetHash([ref('country')]), flow_context());

        foreach ([$whole, $collector] as $each) {
            $each->capture(array_to_rows([['country' => 'PL', 'id' => 1], ['country' => 'DE', 'id' => 2]], $schema));
            $each->capture(array_to_rows([['country' => 'PL', 'id' => 3]], $schema));
        }

        static::assertEquals($whole->report()->datasetHash()?->total, $collector->report()->datasetHash()?->total);
        static::assertEquals(
            [new Partitions(new Partition('country', 'PL')), new Partitions(new Partition('country', 'DE'))],
            array_map(
                static fn(PartitionHash $hash): Partitions => $hash->partitions,
                $collector->report()->datasetHash()->partitions ?? [],
            ),
        );
        static::assertSame(
            [2, 1],
            array_map(
                static fn(PartitionHash $hash): int => $hash->hash->rows,
                $collector->report()->datasetHash()->partitions ?? [],
            ),
        );
    }
}
