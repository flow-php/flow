<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\API\Transactional;
use Flow\ETL\Foundation\Clock\FakeClock;
use Flow\ETL\Operators\Processor\BatchingProcessor;
use Flow\ETL\Operators\Processor\DistinctProcessor;
use Flow\ETL\Operators\Transformer\LimitTransformer;
use Flow\ETL\Operators\Transformer\UntilTransformer;
use Flow\ETL\Storage\FilesystemRowsStorage;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\Double\ThrowingLoader;
use Flow\ETL\Tests\Double\ThrowingTransformer;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\DataFrameMother;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\TracerProvider;
use RuntimeException;

use function array_filter;
use function array_key_exists;
use function array_keys;
use function array_map;
use function array_values;
use function count;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\limit;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\telemetry_options;
use function Flow\ETL\DSL\to_array;
use function Flow\ETL\DSL\to_branch;
use function Flow\ETL\DSL\to_transformation;
use function Flow\ETL\DSL\with_column;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function in_array;
use function str_contains;
use function str_ends_with;
use function str_starts_with;

final class TelemetryTest extends FlowTestCase
{
    public function test_dataframe_collects_metrics_when_enabled(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $clock = new FakeClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        df(config_builder()->withTelemetry($telemetry, telemetry_options(collect_metrics: true))->build())
            ->read(from_array([
                ['id' => 1, 'name' => 'John'],
                ['id' => 2, 'name' => 'Jane'],
                ['id' => 3, 'name' => 'Doe'],
            ]))->run();

        $telemetry->flush();

        $counterMetrics = $metricProcessor->metricsWithName('flow.etl.rows.processed');
        static::assertNotEmpty($counterMetrics, 'Counter metrics should be collected');
        static::assertSame(3, $counterMetrics[0]->value);
    }

    public function test_dataframe_loading_traced_when_enabled(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $clock = new FakeClock();
        $contextStorage = new MemoryContextStorage();

        $config = config_builder()
            ->withTelemetry(
                new Telemetry(
                    Resource::create(['service.name' => 'flow-test']),
                    new TracerProvider($spanProcessor, $clock, $contextStorage),
                    new MeterProvider($metricProcessor, $clock),
                    new LoggerProvider($logProcessor, $clock, $contextStorage),
                ),
                telemetry_options(trace_loading: true),
            )
            ->build();

        $output = [];
        df($config)
            ->read(from_array([
                ['id' => 1, 'name' => 'John'],
                ['id' => 2, 'name' => 'Jane'],
            ]))
            ->write(to_array($output))
            ->run();

        $endedSpans = $spanProcessor->endedSpans();

        $dataFrameSpan = null;
        $loadingSpans = [];

        foreach ($endedSpans as $span) {
            if ($span->name() === 'DataFrame flow_dataframe') {
                $dataFrameSpan = $span;
            } elseif (str_ends_with($span->name(), 'Loader')) {
                $loadingSpans[] = $span;
            }
        }

        static::assertNotNull($dataFrameSpan, 'DataFrame span should be created');
        static::assertNotEmpty($loadingSpans, 'Loading spans should be created when trace_loading is enabled');

        foreach ($loadingSpans as $span) {
            // OTEL spec: instrumentation leaves the status Unset on success.
            static::assertNull($span->status());
            static::assertArrayHasKey('flow.etl.loader.class', $span->attributes());
        }
    }

    public function test_dataframe_run_creates_telemetry_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $clock = new FakeClock();
        $contextStorage = new MemoryContextStorage();

        df(
            config_builder()
                ->withTelemetry(
                    new Telemetry(
                        Resource::create(['service.name' => 'flow-test']),
                        new TracerProvider($spanProcessor, $clock, $contextStorage),
                        new MeterProvider($metricProcessor, $clock),
                        new LoggerProvider($logProcessor, $clock, $contextStorage),
                    ),
                )
                ->build(),
        )
            ->read(from_array([
                ['id' => 1, 'name' => 'John'],
                ['id' => 2, 'name' => 'Jane'],
            ]))->run();

        $endedSpans = $spanProcessor->endedSpans();
        static::assertCount(1, $endedSpans);

        $dataFrameSpan = $endedSpans[0];
        static::assertSame('DataFrame flow_dataframe', $dataFrameSpan->name());
        // OTEL spec: instrumentation leaves the status Unset on success.
        static::assertNull($dataFrameSpan->status());
    }

    public function test_pipeline_start_log_reports_spill_storages(): void
    {
        $context = new MemoryTelemetryContext();

        df($context->config)->read(from_array([['id' => 1]]))->run();

        $started = $context->logs->entriesContaining('Data frame processing started');

        static::assertCount(1, $started);

        $attributes = $started[0]->record->attributes;

        static::assertFalse($attributes->has('fstab'));
        static::assertTrue($attributes->has('spill'));

        $spill = type_map(type_string(), type_string())->assert($attributes->get('spill'));

        static::assertSame(['sort', 'group_by', 'join'], array_keys($spill));
        static::assertSame(FilesystemRowsStorage::class, $spill['sort']);
    }

    public function test_dataframe_run_logs_start_and_completion(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $clock = new FakeClock();
        $contextStorage = new MemoryContextStorage();

        df(
            config_builder()
                ->withTelemetry(
                    new Telemetry(
                        Resource::create(['service.name' => 'flow-test']),
                        new TracerProvider($spanProcessor, $clock, $contextStorage),
                        new MeterProvider($metricProcessor, $clock),
                        new LoggerProvider($logProcessor, $clock, $contextStorage),
                    ),
                )
                ->build(),
        )
            ->read(from_array([
                ['id' => 1, 'name' => 'John'],
                ['id' => 2, 'name' => 'Jane'],
            ]))->run();

        $debugLogs = $logProcessor->entriesWithSeverity(Severity::DEBUG);
        static::assertGreaterThanOrEqual(2, count($debugLogs));

        $startLog = null;
        $completionLog = null;

        foreach ($debugLogs as $log) {
            if (str_contains($log->record->body, 'started')) {
                $startLog = $log;
            }

            if (str_contains($log->record->body, 'completed')) {
                $completionLog = $log;
            }
        }

        static::assertNotNull($startLog, 'Start log should be recorded');
        static::assertNotNull($completionLog, 'Completion log should be recorded');
    }

    public function test_dataframe_span_contains_row_statistics(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $clock = new FakeClock();
        $contextStorage = new MemoryContextStorage();

        df(
            config_builder()
                ->withTelemetry(
                    new Telemetry(
                        Resource::create(['service.name' => 'flow-test']),
                        new TracerProvider($spanProcessor, $clock, $contextStorage),
                        new MeterProvider($metricProcessor, $clock),
                        new LoggerProvider($logProcessor, $clock, $contextStorage),
                    ),
                )
                ->build(),
        )
            ->read(from_array([
                ['id' => 1, 'name' => 'John'],
                ['id' => 2, 'name' => 'Jane'],
                ['id' => 3, 'name' => 'Doe'],
                ['id' => 4, 'name' => 'Smith'],
                ['id' => 5, 'name' => 'Brown'],
            ]))->run();

        $endedSpans = $spanProcessor->endedSpans();
        static::assertCount(1, $endedSpans);

        $attributes = $endedSpans[0]->attributes();

        static::assertArrayHasKey('flow.etl.rows.total', $attributes);
        static::assertSame(5, $attributes['flow.etl.rows.total']);
        static::assertArrayHasKey('flow.etl.memory.min', $attributes);
        static::assertArrayHasKey('flow.etl.memory.max', $attributes);
    }

    public function test_dataframe_transformations_traced_when_enabled(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $clock = new FakeClock();
        $contextStorage = new MemoryContextStorage();

        df(
            config_builder()
                ->withTelemetry(
                    new Telemetry(
                        Resource::create(['service.name' => 'flow-test']),
                        new TracerProvider($spanProcessor, $clock, $contextStorage),
                        new MeterProvider($metricProcessor, $clock),
                        new LoggerProvider($logProcessor, $clock, $contextStorage),
                    ),
                    telemetry_options(trace_transformations: true),
                )
                ->build(),
        )
            ->read(from_array([
                ['id' => 1, 'name' => 'John'],
                ['id' => 2, 'name' => 'Jane'],
            ]))
            ->withColumn('upper_name', ref('name')->upper())
            ->limit(10)
            ->run();

        $endedSpans = $spanProcessor->endedSpans();

        $dataFrameSpan = null;
        $transformerSpans = [];

        foreach ($endedSpans as $span) {
            if ($span->name() === 'DataFrame flow_dataframe') {
                $dataFrameSpan = $span;
            } elseif (str_ends_with($span->name(), 'Transformer')) {
                $transformerSpans[] = $span;
            }
        }

        static::assertNotNull($dataFrameSpan, 'DataFrame span should be created');
        static::assertNotEmpty(
            $transformerSpans,
            'Transformer spans should be created when trace_transformations is enabled',
        );

        foreach ($transformerSpans as $span) {
            // OTEL spec: instrumentation leaves the status Unset on success.
            static::assertNull($span->status());
            static::assertArrayHasKey('flow.etl.transformer.class', $span->attributes());
        }
    }

    public function test_each_operator_of_a_drained_chain_closes_its_own_span_under_the_dataframe_span(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        df($context->config)
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 2], ['id' => 3]])->withBatchSize(1))
            ->batchSize(2)
            ->dropDuplicates(ref('id'))
            ->fetch();

        $dataFrameSpan = array_values(array_filter(
            $context->spans->endedSpans(),
            static fn(Span $span): bool => $span->name() === 'DataFrame flow_dataframe',
        ))[0];

        static::assertSame(
            [
                ['BatchingProcessor', BatchingProcessor::class, true, null],
                ['DistinctProcessor', DistinctProcessor::class, true, null],
            ],
            array_values(array_map(
                static fn(Span $span): array => [
                    $span->name(),
                    $span->attributes()['flow.etl.operator.class'],
                    $span->context()->parentSpanId?->toHex() === $dataFrameSpan->context()->spanId->toHex(),
                    $span->status(),
                ],
                array_filter($context->spans->endedSpans(), static fn(Span $span): bool => array_key_exists(
                    'flow.etl.operator.class',
                    $span->attributes(),
                )),
            )),
        );
        static::assertNull($context->telemetry->tracer('flow-php')->context()->activeSpan());
    }

    public function test_each_operator_of_a_chain_a_limit_stops_closes_its_own_span(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        df($context->config)
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 2], ['id' => 3]])->withBatchSize(1))
            ->batchSize(1)
            ->dropDuplicates(ref('id'))
            ->withColumn('x', lit(1))
            ->limit(1)
            ->fetch();

        static::assertSame(
            [
                ['BatchingProcessor', BatchingProcessor::class, null],
                ['DistinctProcessor', DistinctProcessor::class, null],
            ],
            array_values(array_map(
                static fn(Span $span): array => [
                    $span->name(),
                    $span->attributes()['flow.etl.operator.class'],
                    $span->status(),
                ],
                array_filter($context->spans->endedSpans(), static fn(Span $span): bool => array_key_exists(
                    'flow.etl.operator.class',
                    $span->attributes(),
                )),
            )),
        );
        static::assertNull($context->telemetry->tracer('flow-php')->context()->activeSpan());
    }

    public function test_each_operator_of_a_chain_its_consumer_abandons_closes_its_own_span(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        foreach (df($context->config)
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 2], ['id' => 3]])->withBatchSize(1))
            ->batchSize(1)
            ->dropDuplicates(ref('id'))
            ->get() as $_rows) {
            break;
        }

        static::assertSame(
            [
                ['DistinctProcessor',        DistinctProcessor::class, null],
                ['BatchingProcessor',        BatchingProcessor::class, null],
                ['DataFrame flow_dataframe', null,                     null],
            ],
            array_map(static fn(Span $span): array => [
                $span->name(),
                $span->attributes()['flow.etl.operator.class'] ?? null,
                $span->status(),
            ], $context->spans->endedSpans()),
        );
        static::assertNull($context->telemetry->tracer('flow-php')->context()->activeSpan());
    }

    public function test_duplicate_row_transformer_opens_no_nested_span(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        $output = [];
        df($context->config)
            ->read(from_array([['id' => 1], ['id' => 2]])->withBatchSize(1))
            ->duplicateRow(condition: lit(true), columns: with_column('id', ref('id')->multiply(lit(-1))))
            ->write(to_array($output))
            ->run();

        $endedSpans = $context->spans->endedSpans();

        static::assertCount(2, array_filter(
            $endedSpans,
            static fn(Span $span): bool => $span->name() === 'DuplicateRowTransformer',
        ));
        static::assertCount(0, array_filter(
            $endedSpans,
            static fn(Span $span): bool => $span->name() === 'ScalarFunctionTransformer',
        ));
    }

    public function test_limit_reached_inside_a_branch_is_not_reported_as_failure(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_loading: true));

        $rows = [];

        for ($id = 1; $id <= 20; $id++) {
            $rows[] = ['id' => $id];
        }

        $output = [];
        df($context->config)
            ->read(from_array($rows))
            ->write(to_transformation(new LimitTransformer(10), to_array($output)))
            ->run();

        static::assertEmpty($context->logs->entriesContaining('Loading failed'));
        static::assertEmpty($context->logs->entriesContaining('Error during ETL pipeline execution.'));
        static::assertEmpty($context->logs->entriesContaining('Data frame processing failed'));
        static::assertCount(1, $context->logs->entriesContaining('Limit reached'));
        static::assertEmpty(array_filter(
            $context->spans->endedSpans(),
            static fn(Span $span): bool => $span->status()?->isError() === true,
        ));
    }

    public function test_limit_reached_inside_transformation_is_logged_once_across_batches(): void
    {
        $context = new MemoryTelemetryContext();

        $source = [];

        for ($id = 1; $id <= 20; $id++) {
            $source[] = ['id' => $id];
        }

        $output = [];
        df($context->config)
            ->read(from_array($source))
            ->write(to_transformation(limit(3), to_array($output)))
            ->run();

        static::assertCount(3, $output);
        static::assertCount(1, $context->logs->entriesContaining('Limit reached'));
    }

    public function test_limit_reached_is_logged_with_the_step_that_reached_it(): void
    {
        $context = new MemoryTelemetryContext();

        $rows = [];

        for ($id = 1; $id <= 20; $id++) {
            $rows[] = ['id' => $id];
        }

        df($context->config)->read(from_array($rows))->limit(5)->run();

        $logEntries = $context->logs->entriesContaining('Limit reached');

        static::assertCount(1, $logEntries);
        static::assertFalse($logEntries[0]->record->attributes->has('limit_exception'));
        static::assertSame(LimitTransformer::class, $logEntries[0]->record->attributes->get('step'));
    }

    public function test_telemetry_disabled_by_default_uses_void_providers(): void
    {
        $config = config_builder()->build();

        $output = [];
        df($config)
            ->read(from_array([
                ['id' => 1, 'name' => 'John'],
            ]))
            ->write(to_array($output))
            ->run();

        static::assertCount(1, $output);
        static::assertSame(1, $output[0]['id']);
    }

    public function test_loading_rows_on_a_branch_counts_the_rows_written(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_loading: true));

        $output = [];
        df($context->config)
            ->read(from_rows(...RowsMother::descendingIdBatches()))
            ->write(to_branch(ref('id')->greaterThanEqual(lit(2)), to_array($output)))
            ->run();

        static::assertSame(
            [2, 2],
            array_values(array_map(
                static fn(Span $span): mixed => $span->attributes()['flow.etl.loading.rows'],
                array_filter(
                    $context->spans->endedSpans(),
                    static fn(Span $span): bool => $span->name() === 'ArrayLoader',
                ),
            )),
        );
    }

    public function test_planner_built_write_steps_emit_no_span(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_loading: true));

        $output = [];
        df($context->config)
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write(new Transactional(new RecordingTransaction(), to_transformation(limit(1), to_array($output))))
            ->run();

        static::assertCount(1, $output);
        static::assertSame(
            [],
            array_values(array_filter($context->spans->endedSpans(), static fn(Span $span): bool => in_array(
                $span->name(),
                ['BranchLoader', 'TransactionLoader'],
                true,
            ))),
        );
    }

    public function test_a_branch_builds_no_dataframe_span_of_its_own(): void
    {
        $context = new MemoryTelemetryContext();

        $source = [];

        for ($id = 1; $id <= 6; $id++) {
            $source[] = ['id' => $id];
        }

        $output = [];
        df($context->config)
            ->read(from_array($source))
            ->write(to_transformation(limit(3), to_array($output)))
            ->run();

        $isDataFrameSpan = static fn(Span $span): bool => str_starts_with($span->name(), 'DataFrame ');

        static::assertCount(1, array_filter($context->spans->startedSpans(), $isDataFrameSpan));
        static::assertCount(1, array_filter($context->spans->endedSpans(), $isDataFrameSpan));
    }

    public function test_until_condition_is_logged_with_the_step_that_reached_it(): void
    {
        $context = new MemoryTelemetryContext();

        $rows = [];

        for ($id = 1; $id <= 20; $id++) {
            $rows[] = ['id' => $id];
        }

        df($context->config)
            ->read(from_array($rows))
            ->until(ref('id')->lessThan(lit(5)))
            ->run();

        $logEntries = $context->logs->entriesContaining('Limit reached');

        static::assertCount(1, $logEntries);
        static::assertFalse($logEntries[0]->record->attributes->has('limit_exception'));
        static::assertSame(UntilTransformer::class, $logEntries[0]->record->attributes->get('step'));
    }

    public function test_a_join_builds_one_balanced_dataframe_span_per_run(): void
    {
        $context = new MemoryTelemetryContext();
        df($context->config)
            ->read(from_array([['id' => 1]]))
            ->join(df($context->config)->read(from_array([['id' => 1, 'name' => 'a']])), join_on(['id' => 'id'], 'r_'))
            ->run();

        $isDataFrameSpan = static fn(Span $span): bool => str_starts_with($span->name(), 'DataFrame ');

        static::assertCount(1, array_filter($context->spans->startedSpans(), $isDataFrameSpan));
        static::assertCount(1, array_filter($context->spans->endedSpans(), $isDataFrameSpan));
    }

    public function test_a_join_over_frames_sharing_one_flow_context_builds_one_balanced_dataframe_span(): void
    {
        $context = new MemoryTelemetryContext();
        DataFrameMother::over(from_array([['id' => 1]]), $context->flowContext)->join(
            DataFrameMother::over(from_array([['id' => 1, 'x' => 'a']]), $context->flowContext),
            join_on([
                'id' => 'id',
            ], 'r_'),
        )->run();

        $isDataFrameSpan = static fn(Span $span): bool => str_starts_with($span->name(), 'DataFrame ');

        static::assertCount(1, array_filter($context->spans->startedSpans(), $isDataFrameSpan));
        static::assertCount(1, array_filter($context->spans->endedSpans(), $isDataFrameSpan));
    }

    public function test_a_cross_join_builds_one_balanced_dataframe_span_per_run(): void
    {
        $context = new MemoryTelemetryContext();
        df($context->config)
            ->read(from_array([['id' => 1]]))
            ->crossJoin(df($context->config)->read(from_array([['name' => 'a']])), 'r_')
            ->run();

        $isDataFrameSpan = static fn(Span $span): bool => str_starts_with($span->name(), 'DataFrame ');

        static::assertCount(1, array_filter($context->spans->startedSpans(), $isDataFrameSpan));
        static::assertCount(1, array_filter($context->spans->endedSpans(), $isDataFrameSpan));
    }

    public function test_a_frame_run_twice_builds_two_balanced_dataframe_spans(): void
    {
        $context = new MemoryTelemetryContext();
        $frame = df($context->config)->read(from_array([['id' => 1]]));

        $frame->count();
        $frame->fetch();

        $isDataFrameSpan = static fn(Span $span): bool => str_starts_with($span->name(), 'DataFrame ');

        static::assertCount(2, array_filter($context->spans->startedSpans(), $isDataFrameSpan));
        static::assertCount(2, array_filter($context->spans->endedSpans(), $isDataFrameSpan));
    }

    public function test_a_run_with_a_transformer_a_sort_and_a_loader_emits_these_signals(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(
            trace_loading: true,
            trace_transformations: true,
            collect_metrics: true,
        ));
        $output = [];

        df($telemetry->config)
            ->read(from_array([['id' => 3], ['id' => 1], ['id' => 2]]))
            ->withColumn('flag', lit(true))
            ->sortBy(ref('id'))
            ->write(to_array($output))
            ->run();

        static::assertSame(
            [
                'spans' => [
                    [
                        'ScalarFunctionTransformer',
                        'INTERNAL',
                        null,
                        [
                            'flow.etl.dataframe.name',
                            'flow.etl.scalar.function',
                            'flow.etl.transformation.input_rows',
                            'flow.etl.transformation.output_rows',
                            'flow.etl.transformer.class',
                        ],
                    ],
                    [
                        'ArrayLoader',
                        'INTERNAL',
                        null,
                        [
                            'flow.etl.dataframe.name',
                            'flow.etl.loader.class',
                            'flow.etl.loading.rows',
                        ],
                    ],
                    [
                        'MergeSortProcessor',
                        'INTERNAL',
                        null,
                        [
                            'flow.etl.dataframe.name',
                            'flow.etl.operator.class',
                        ],
                    ],
                    [
                        'DataFrame flow_dataframe',
                        'INTERNAL',
                        null,
                        [
                            'flow.etl.dataframe.id',
                            'flow.etl.dataframe.name',
                            'flow.etl.memory.max',
                            'flow.etl.memory.min',
                            'flow.etl.rows.throughput.per_second',
                            'flow.etl.rows.total',
                        ],
                    ],
                ],
                'logs' => [
                    [
                        5,
                        'Data frame processing started',
                        [
                            'cache',
                            'dataframe_id',
                            'dataframe_name',
                            'optimizer_rules',
                            'serializer',
                            'spill',
                            'telemetry',
                        ],
                    ],
                    [
                        5,
                        'Data frame processing completed',
                        [
                            'dataframe_id',
                            'dataframe_name',
                            'memory_max_mb',
                            'memory_min_mb',
                            'total_rows_processed',
                        ],
                    ],
                ],
                'metrics' => [
                    ['flow.etl.rows.processed', 'counter', ['flow.etl.dataframe.name']],
                    ['flow.etl.rows.throughput', 'gauge', ['flow.etl.dataframe.name']],
                ],
            ],
            $telemetry->signals(),
        );
    }

    public function test_a_failing_transformer_emits_these_signals(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(
            trace_loading: true,
            trace_transformations: true,
            collect_metrics: true,
        ));

        try {
            df($telemetry->config)
                ->read(from_array([['id' => 1]]))
                ->with(new ThrowingTransformer(new RuntimeException('boom')))
                ->run();
            static::fail('expected the transformer to fail the run');
        } catch (RuntimeException) {
        }

        static::assertSame(
            [
                'spans' => [
                    [
                        'ThrowingTransformer',
                        'INTERNAL',
                        'ERROR',
                        [
                            'error.type',
                            'flow.etl.dataframe.name',
                            'flow.etl.transformer.class',
                        ],
                    ],
                    [
                        'DataFrame flow_dataframe',
                        'INTERNAL',
                        'ERROR',
                        [
                            'error.type',
                            'flow.etl.dataframe.id',
                            'flow.etl.dataframe.name',
                            'flow.etl.memory.max',
                            'flow.etl.memory.min',
                            'flow.etl.rows.throughput.per_second',
                            'flow.etl.rows.total',
                        ],
                    ],
                ],
                'logs' => [
                    [
                        5,
                        'Data frame processing started',
                        [
                            'cache',
                            'dataframe_id',
                            'dataframe_name',
                            'optimizer_rules',
                            'serializer',
                            'spill',
                            'telemetry',
                        ],
                    ],
                    [17, 'Transformation failed', ['exception', 'transformer']],
                    [17, 'Error during ETL pipeline execution.', ['exception']],
                    [17, 'Data frame processing failed', ['dataframe_id', 'dataframe_name', 'exception']],
                ],
                'metrics' => [],
            ],
            $telemetry->signals(),
        );
    }

    public function test_a_failing_loader_emits_these_signals(): void
    {
        $telemetry = new MemoryTelemetryContext(telemetry_options(
            trace_loading: true,
            trace_transformations: true,
            collect_metrics: true,
        ));

        try {
            df($telemetry->config)
                ->read(from_array([['id' => 1]]))
                ->write(new ThrowingLoader(new RuntimeException('boom')))
                ->run();
            static::fail('expected the loader to fail the run');
        } catch (RuntimeException) {
        }

        static::assertSame(
            [
                'spans' => [
                    [
                        'ThrowingLoader',
                        'INTERNAL',
                        'ERROR',
                        [
                            'error.type',
                            'flow.etl.dataframe.name',
                            'flow.etl.loader.class',
                        ],
                    ],
                    [
                        'DataFrame flow_dataframe',
                        'INTERNAL',
                        'ERROR',
                        [
                            'error.type',
                            'flow.etl.dataframe.id',
                            'flow.etl.dataframe.name',
                            'flow.etl.memory.max',
                            'flow.etl.memory.min',
                            'flow.etl.rows.throughput.per_second',
                            'flow.etl.rows.total',
                        ],
                    ],
                ],
                'logs' => [
                    [
                        5,
                        'Data frame processing started',
                        [
                            'cache',
                            'dataframe_id',
                            'dataframe_name',
                            'optimizer_rules',
                            'serializer',
                            'spill',
                            'telemetry',
                        ],
                    ],
                    [17, 'Loading failed', ['exception', 'loader']],
                    [17, 'Error during ETL pipeline execution.', ['exception']],
                    [17, 'Data frame processing failed', ['dataframe_id', 'dataframe_name', 'exception']],
                ],
                'metrics' => [],
            ],
            $telemetry->signals(),
        );
    }
}
