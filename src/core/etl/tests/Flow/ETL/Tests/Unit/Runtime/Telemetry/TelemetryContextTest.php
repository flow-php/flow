<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Telemetry;

use Flow\ETL\Foundation\Clock\FakeClock;
use Flow\ETL\IO\Loader\ArrayLoader;
use Flow\ETL\IO\Loader\StreamLoader;
use Flow\ETL\Operators\Processor\BatchingProcessor;
use Flow\ETL\Operators\Processor\DistinctProcessor;
use Flow\ETL\Operators\Transformer\LimitTransformer;
use Flow\ETL\Operators\Transformer\UntilTransformer;
use Flow\ETL\Planning\Optimizer\Rule\CombineLimits;
use Flow\ETL\Planning\Optimizer\Rule\CombineSortAndLimit;
use Flow\ETL\Planning\Optimizer\Rule\CountFromStatistics;
use Flow\ETL\Planning\Optimizer\Rule\PushFilterIntoSource;
use Flow\ETL\Planning\Optimizer\Rule\PushLimitIntoSource;
use Flow\ETL\Planning\Optimizer\Rule\PushOffsetIntoSource;
use Flow\ETL\Planning\Optimizer\Rule\PushProjectionIntoExpand;
use Flow\ETL\Runtime\Telemetry\StepKind;
use Flow\ETL\Runtime\Telemetry\TelemetryContext;
use Flow\ETL\Runtime\Telemetry\TelemetryOptions;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
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

use function array_map;
use function count;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\telemetry_options;

final class TelemetryContextTest extends FlowTestCase
{
    public function test_dataframe_batch_processed_drains_abandoned_spans(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        $context->telemetryContext->dataFrameStarted([]);

        $context->telemetryContext->stepStarted(StepKind::transformation, LimitTransformer::class);
        $context->telemetryContext->stepCompleted($context->telemetryContext->stepStarted(
            StepKind::transformation,
            UntilTransformer::class,
        ));

        $context->telemetryContext->dataFrameBatchProcessed(array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id'))));

        $endedSpans = $context->spans->endedSpans();

        static::assertSame(
            ['UntilTransformer', 'LimitTransformer'],
            array_map(static fn(Span $span): string => $span->name(), $endedSpans),
        );
        static::assertNull($endedSpans[0]->status());

        $status = $endedSpans[1]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('Span was never completed.', $status->description);
    }

    public function test_an_operator_span_outlives_a_processed_batch_and_is_drained_when_the_dataframe_ends(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        $context->telemetryContext->dataFrameStarted([]);
        $context->telemetryContext->stepStarted(StepKind::operator, DistinctProcessor::class);
        $context->telemetryContext->dataFrameBatchProcessed(array_to_rows([['id' => 1]], schema(int_schema('id'))));

        static::assertEmpty($context->spans->endedSpans());

        $context->telemetryContext->dataFrameCompleted();

        static::assertSame(
            [
                ['DistinctProcessor',        'Span was never completed.'],
                ['DataFrame flow_dataframe', null],
            ],
            array_map(static fn(Span $span): array => [
                $span->name(),
                $span->status()?->description,
            ], $context->spans->endedSpans()),
        );
    }

    public function test_dataframe_batch_processed_tracks_rows_and_memory(): void
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

        $telemetryContext = new TelemetryContext(
            'id',
            'flow_dataframe',
            new PhpBackend(),
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            telemetry_options(collect_metrics: true),
        );

        $telemetryContext->dataFrameStarted([]);

        $telemetryContext->dataFrameBatchProcessed(array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
        ], schema(int_schema('id'))));

        $telemetryContext->dataFrameCompleted();
        $telemetry->flush();

        static::assertNotEmpty($metricProcessor->metrics());

        $counterMetrics = $metricProcessor->metricsWithName('flow.etl.rows.processed');
        static::assertNotEmpty($counterMetrics, 'Counter metrics should be collected');
        static::assertSame(3, $counterMetrics[0]->value);
    }

    public function test_dataframe_completed_drains_abandoned_spans(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        $context->telemetryContext->dataFrameStarted([]);

        $context->telemetryContext->stepStarted(StepKind::transformation, LimitTransformer::class);
        $context->telemetryContext->stepCompleted($context->telemetryContext->stepStarted(
            StepKind::transformation,
            UntilTransformer::class,
        ));

        $context->telemetryContext->dataFrameCompleted();

        $endedSpans = $context->spans->endedSpans();

        static::assertSame(
            ['UntilTransformer', 'LimitTransformer', 'DataFrame flow_dataframe'],
            array_map(static fn(Span $span): string => $span->name(), $endedSpans),
        );

        $status = $endedSpans[1]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('Span was never completed.', $status->description);
        static::assertNull($endedSpans[2]->status());
    }

    public function test_dataframe_completed_finalizes_span_with_statistics(): void
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

        $telemetryContext = new TelemetryContext(
            'id',
            'flow_dataframe',
            new PhpBackend(),
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            new TelemetryOptions(),
        );

        $telemetryContext->dataFrameStarted([]);

        $telemetryContext->dataFrameBatchProcessed(array_to_rows([
            ['id' => 1],
            ['id' => 2],
        ], schema(int_schema('id'))));
        $telemetryContext->dataFrameCompleted();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $span = $spans[0];
        static::assertSame('DataFrame flow_dataframe', $span->name());
        // OTEL spec: instrumentation leaves the status Unset on success.
        static::assertNull($span->status());

        $attributes = $span->attributes();
        static::assertSame('id', $attributes['flow.etl.dataframe.id']);
        static::assertSame(2, $attributes['flow.etl.rows.total']);
        static::assertArrayHasKey('flow.etl.memory.min', $attributes);
        static::assertArrayHasKey('flow.etl.memory.max', $attributes);

        static::assertGreaterThanOrEqual(2, count($logProcessor->entriesWithSeverity(Severity::DEBUG)));
    }

    public function test_dataframe_failed_drains_abandoned_spans(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        $context->telemetryContext->dataFrameStarted([]);

        $context->telemetryContext->stepStarted(StepKind::transformation, LimitTransformer::class);
        $context->telemetryContext->stepCompleted($context->telemetryContext->stepStarted(
            StepKind::transformation,
            UntilTransformer::class,
        ));

        $context->telemetryContext->dataFrameFailed(new RuntimeException('boom'));

        $endedSpans = $context->spans->endedSpans();

        static::assertSame(
            ['UntilTransformer', 'LimitTransformer', 'DataFrame flow_dataframe'],
            array_map(static fn(Span $span): string => $span->name(), $endedSpans),
        );
        static::assertSame('Span was never completed.', $endedSpans[1]->status()?->description);
        static::assertSame('boom', $endedSpans[2]->status()?->description);
    }

    public function test_dataframe_failed_logs_error_and_sets_span_status(): void
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

        $telemetryContext = new TelemetryContext(
            'id',
            'flow_dataframe',
            new PhpBackend(),
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            new TelemetryOptions(),
        );

        $telemetryContext->dataFrameStarted([]);

        $telemetryContext->dataFrameBatchProcessed(array_to_rows([['id' => 1]], schema(int_schema('id'))));

        $telemetryContext->dataFrameFailed(new RuntimeException('Processing failed due to invalid data'));

        $errorLogs = $logProcessor->entriesWithSeverity(Severity::ERROR);
        static::assertCount(1, $errorLogs);
        static::assertStringContainsString('Data frame processing failed', $errorLogs[0]->record->body);
        static::assertSame('id', $errorLogs[0]->record->attributes->get('dataframe_id'));

        $endedSpans = $spanProcessor->endedSpans();
        static::assertCount(1, $endedSpans);
        $status = $endedSpans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('Processing failed due to invalid data', $status->description);
        static::assertSame(RuntimeException::class, $endedSpans[0]->attributes()['error.type']);

        $attributes = $endedSpans[0]->attributes();
        static::assertArrayHasKey('flow.etl.rows.total', $attributes);
        static::assertSame(1, $attributes['flow.etl.rows.total']);
    }

    public function test_dataframe_started_creates_span_and_logs_the_configuration_with_the_id_and_name(): void
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

        (new TelemetryContext(
            'id',
            'flow_dataframe',
            new PhpBackend(),
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            new TelemetryOptions(),
        ))->dataFrameStarted(['cache' => 'memory']);

        $startedSpans = $spanProcessor->startedSpans();
        static::assertCount(1, $startedSpans);
        static::assertSame('DataFrame flow_dataframe', $startedSpans[0]->name());

        $debugLogs = $logProcessor->entriesWithSeverity(Severity::DEBUG);
        static::assertCount(1, $debugLogs);
        static::assertSame('Data frame processing started', $debugLogs[0]->record->body);
        static::assertSame(
            ['dataframe_id' => 'id', 'dataframe_name' => 'flow_dataframe', 'cache' => 'memory'],
            $debugLogs[0]->record->attributes->all(),
        );
    }

    public function test_limit_reached_logs_debug_message_without_attributes(): void
    {
        $context = new MemoryTelemetryContext();

        $context->telemetryContext->limitReached();

        $debugLogs = $context->logs->entriesWithSeverity(Severity::DEBUG);

        static::assertCount(1, $debugLogs);
        static::assertSame('Limit reached, stopping the pipeline execution.', $debugLogs[0]->record->body);
        static::assertTrue($debugLogs[0]->record->attributes->isEmpty());
    }

    public function test_limit_reached_passes_through_attributes(): void
    {
        $context = new MemoryTelemetryContext();

        $context->telemetryContext->limitReached(['limit' => 10]);

        static::assertSame(10, $context->logs->entriesContaining('Limit reached')[0]->record->attributes->get('limit'));
    }

    public function test_a_loading_step_completes_with_its_loader_class_and_no_status(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_loading: true));

        $context->telemetryContext->dataFrameStarted([]);
        $context->telemetryContext->stepCompleted($context->telemetryContext->stepStarted(
            StepKind::loading,
            StreamLoader::class,
        ));

        $endedSpans = $context->spans->endedSpans();

        static::assertCount(1, $endedSpans);
        static::assertSame('StreamLoader', $endedSpans[0]->name());
        static::assertSame(StreamLoader::class, $endedSpans[0]->attributes()['flow.etl.loader.class']);
        static::assertSame('flow_dataframe', $endedSpans[0]->attributes()['flow.etl.dataframe.name']);
        // OTEL spec: instrumentation leaves the status Unset on success.
        static::assertNull($endedSpans[0]->status());
    }

    public function test_a_step_completed_twice_ends_its_span_once(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        $context->telemetryContext->dataFrameStarted([]);
        $step = $context->telemetryContext->stepStarted(StepKind::transformation, LimitTransformer::class);
        $context->telemetryContext->stepCompleted($step);
        $context->telemetryContext->stepCompleted($step);
        $context->telemetryContext->stepFailed(
            $step,
            StepKind::transformation,
            LimitTransformer::class,
            new RuntimeException('late'),
        );

        static::assertCount(1, $context->spans->endedSpans());
        static::assertEmpty($context->logs->entriesWithSeverity(Severity::ERROR));
    }

    public function test_a_loading_step_failure_logs_the_loader_and_sets_span_status(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_loading: true));

        $context->telemetryContext->dataFrameStarted([]);
        $context->telemetryContext->stepFailed(
            $context->telemetryContext->stepStarted(StepKind::loading, StreamLoader::class),
            StepKind::loading,
            StreamLoader::class,
            new RuntimeException('Loading failed due to disk error'),
        );

        $errorLogs = $context->logs->entriesWithSeverity(Severity::ERROR);
        static::assertCount(1, $errorLogs);
        static::assertSame('Loading failed', $errorLogs[0]->record->body);
        static::assertSame(StreamLoader::class, $errorLogs[0]->record->attributes->get('loader'));

        $endedSpans = $context->spans->endedSpans();
        static::assertCount(1, $endedSpans);
        $status = $endedSpans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('Loading failed due to disk error', $status->description);
        static::assertSame(RuntimeException::class, $endedSpans[0]->attributes()['error.type']);
    }

    public function test_a_transformation_step_failure_logs_the_transformer_and_sets_span_status(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        $context->telemetryContext->dataFrameStarted([]);
        $context->telemetryContext->stepFailed(
            $context->telemetryContext->stepStarted(StepKind::transformation, LimitTransformer::class),
            StepKind::transformation,
            LimitTransformer::class,
            new RuntimeException('Transformation failed'),
        );

        $errorLogs = $context->logs->entriesWithSeverity(Severity::ERROR);
        static::assertCount(1, $errorLogs);
        static::assertSame('Transformation failed', $errorLogs[0]->record->body);
        static::assertSame(LimitTransformer::class, $errorLogs[0]->record->attributes->get('transformer'));

        $endedSpans = $context->spans->endedSpans();
        static::assertCount(1, $endedSpans);
        static::assertSame(LimitTransformer::class, $endedSpans[0]->attributes()['flow.etl.transformer.class']);
        static::assertSame('Transformation failed', $endedSpans[0]->status()?->description);
        static::assertSame(RuntimeException::class, $endedSpans[0]->attributes()['error.type']);
    }

    public function test_an_operator_step_failure_logs_the_operator_and_sets_span_status(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        $context->telemetryContext->dataFrameStarted([]);
        $context->telemetryContext->stepFailed(
            $context->telemetryContext->stepStarted(StepKind::operator, DistinctProcessor::class),
            StepKind::operator,
            DistinctProcessor::class,
            new RuntimeException('Operator broke'),
        );

        $errorLogs = $context->logs->entriesWithSeverity(Severity::ERROR);
        static::assertCount(1, $errorLogs);
        static::assertSame('Operator failed', $errorLogs[0]->record->body);
        static::assertSame(DistinctProcessor::class, $errorLogs[0]->record->attributes->get('operator'));

        $endedSpans = $context->spans->endedSpans();
        static::assertCount(1, $endedSpans);
        static::assertSame('DistinctProcessor', $endedSpans[0]->name());
        static::assertSame(DistinctProcessor::class, $endedSpans[0]->attributes()['flow.etl.operator.class']);
        static::assertSame('Operator broke', $endedSpans[0]->status()?->description);
    }

    public function test_a_step_failure_ends_only_its_own_span(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_loading: true));

        $context->telemetryContext->dataFrameStarted([]);

        $outer = $context->telemetryContext->stepStarted(StepKind::loading, StreamLoader::class);
        $context->telemetryContext->stepFailed(
            $context->telemetryContext->stepStarted(StepKind::loading, ArrayLoader::class),
            StepKind::loading,
            ArrayLoader::class,
            new RuntimeException('Loading failed'),
        );
        $context->telemetryContext->stepCompleted($outer);

        $endedSpans = $context->spans->endedSpans();

        static::assertSame(
            ['ArrayLoader', 'StreamLoader'],
            array_map(static fn(Span $span): string => $span->name(), $endedSpans),
        );
        static::assertTrue($endedSpans[0]->status()?->isError());
        static::assertNull($endedSpans[1]->status());
    }

    public function test_steps_completed_out_of_order_each_end_their_own_span(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        $context->telemetryContext->dataFrameStarted([]);

        $downstream = $context->telemetryContext->stepStarted(StepKind::operator, DistinctProcessor::class);
        $upstream = $context->telemetryContext->stepStarted(StepKind::operator, BatchingProcessor::class);
        $context->telemetryContext->stepCompleted($downstream, ['position' => 'downstream']);

        static::assertSame(
            [['DistinctProcessor', 'downstream']],
            array_map(static fn(Span $span): array => [
                $span->name(),
                $span->attributes()['position'],
            ], $context->spans->endedSpans()),
        );

        $context->telemetryContext->stepCompleted($upstream, ['position' => 'upstream']);

        static::assertSame(
            [
                ['DistinctProcessor', 'downstream'],
                ['BatchingProcessor', 'upstream'],
            ],
            array_map(static fn(Span $span): array => [
                $span->name(),
                $span->attributes()['position'],
            ], $context->spans->endedSpans()),
        );
    }

    public function test_an_operator_span_is_never_activated_a_transformation_span_is(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));
        $tracer = $context->telemetry->tracer('flow-php');

        $context->telemetryContext->dataFrameStarted([]);
        $active = $tracer->activeSpan();
        $operator = $context->telemetryContext->stepStarted(StepKind::operator, DistinctProcessor::class);

        static::assertEquals($active, $tracer->activeSpan());

        $transformation = $context->telemetryContext->stepStarted(StepKind::transformation, LimitTransformer::class);

        static::assertEquals($transformation->span->context(), $tracer->activeSpan());

        $context->telemetryContext->stepCompleted($transformation);
        $context->telemetryContext->stepCompleted($operator);

        static::assertEquals($active, $tracer->activeSpan());
    }

    public function test_nested_steps_complete_both_spans_once(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        $context->telemetryContext->dataFrameStarted([]);

        $outer = $context->telemetryContext->stepStarted(StepKind::transformation, LimitTransformer::class);
        $inner = $context->telemetryContext->stepStarted(StepKind::transformation, UntilTransformer::class);
        $context->telemetryContext->stepCompleted($inner, ['nesting' => 'inner']);
        $context->telemetryContext->stepCompleted($outer, ['nesting' => 'outer']);

        $endedSpans = $context->spans->endedSpans();

        static::assertCount(2, $endedSpans);
        static::assertSame('UntilTransformer', $endedSpans[0]->name());
        static::assertSame('inner', $endedSpans[0]->attributes()['nesting']);
        static::assertSame('LimitTransformer', $endedSpans[1]->name());
        static::assertSame('outer', $endedSpans[1]->attributes()['nesting']);
    }

    public function test_metrics_collected_when_collect_metrics_enabled(): void
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

        $telemetryContext = new TelemetryContext(
            'id',
            'flow_dataframe',
            new PhpBackend(),
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            telemetry_options(collect_metrics: true),
        );

        $telemetryContext->dataFrameStarted([]);

        $telemetryContext->dataFrameBatchProcessed(array_to_rows([
            ['id' => 1],
            ['id' => 2],
        ], schema(int_schema('id'))));
        $telemetryContext->dataFrameCompleted();
        $telemetry->flush();

        $counterMetrics = $metricProcessor->metricsWithName('flow.etl.rows.processed');
        $throughputMetrics = $metricProcessor->metricsWithName('flow.etl.rows.throughput');

        static::assertNotEmpty($counterMetrics, 'Counter should be created when metrics enabled');
        static::assertNotEmpty($throughputMetrics, 'Throughput should be created when metrics enabled');
    }

    public function test_metrics_include_dataframe_name_attribute(): void
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

        $telemetryContext = new TelemetryContext(
            'id',
            'my_custom_dataframe',
            new PhpBackend(),
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            telemetry_options(collect_metrics: true),
        );

        $telemetryContext->dataFrameStarted([]);

        $telemetryContext->dataFrameBatchProcessed(array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
        ], schema(int_schema('id'))));
        $telemetryContext->dataFrameCompleted();
        $telemetry->flush();

        $counterMetrics = $metricProcessor->metricsWithName('flow.etl.rows.processed');
        $throughputMetrics = $metricProcessor->metricsWithName('flow.etl.rows.throughput');

        static::assertCount(1, $counterMetrics);
        static::assertSame(3, $counterMetrics[0]->value);
        static::assertSame('{row}', $counterMetrics[0]->unit);
        static::assertSame('my_custom_dataframe', $counterMetrics[0]->attributes->get('flow.etl.dataframe.name'));

        static::assertCount(1, $throughputMetrics);
        static::assertSame('{row}/s', $throughputMetrics[0]->unit);
        static::assertSame('my_custom_dataframe', $throughputMetrics[0]->attributes->get('flow.etl.dataframe.name'));
    }

    public function test_metrics_not_collected_when_collect_metrics_disabled(): void
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

        $telemetryContext = new TelemetryContext(
            'id',
            'flow_dataframe',
            new PhpBackend(),
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            telemetry_options(collect_metrics: false),
        );

        $telemetryContext->dataFrameStarted([]);

        $telemetryContext->dataFrameBatchProcessed(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $telemetryContext->dataFrameCompleted();
        $telemetry->flush();

        static::assertEmpty($metricProcessor->metrics(), 'No metrics should be collected when disabled');
    }

    public function test_a_split_task_span_is_named_split_and_its_index(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        $context->telemetryContext->dataFrameStarted([]);
        $context->telemetryContext->taskCompleted(3, 'split', ['output_rows' => 7]);

        $span = $context->spans->endedSpans()[0];
        static::assertSame('split 3', $span->name());
        static::assertSame(
            ['split' => 3, 'flow.etl.dataframe.name' => 'flow_dataframe', 'output_rows' => 7],
            $span->attributes(),
        );
    }

    public function test_a_bucket_task_span_is_named_bucket_and_its_index(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        $context->telemetryContext->dataFrameStarted([]);
        $context->telemetryContext->taskCompleted(3, 'bucket');

        $span = $context->spans->endedSpans()[0];
        static::assertSame('bucket 3', $span->name());
        static::assertSame(['bucket' => 3, 'flow.etl.dataframe.name' => 'flow_dataframe'], $span->attributes());
    }

    public function test_a_task_completed_without_traced_transformations_emits_no_span(): void
    {
        $context = new MemoryTelemetryContext();

        $context->telemetryContext->taskCompleted(3, 'split');

        static::assertEmpty($context->spans->endedSpans());
    }

    public function test_optimizer_rules_are_logged(): void
    {
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $clock = new FakeClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider(new MemorySpanProcessor(new VoidExporter()), $clock, $contextStorage),
            new MeterProvider(new MemoryMetricProcessor(new VoidExporter()), $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        (new TelemetryContext(
            'id',
            'flow_dataframe',
            new PhpBackend(),
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            new TelemetryOptions(),
        ))->dataFrameStarted(config()->telemetryAttributes());

        $debugLogs = $logProcessor->entriesWithSeverity(Severity::DEBUG);

        static::assertCount(1, $debugLogs);
        static::assertSame(
            [
                CombineLimits::class,
                CombineSortAndLimit::class,
                PushFilterIntoSource::class,
                PushOffsetIntoSource::class,
                PushLimitIntoSource::class,
                CountFromStatistics::class,
                PushProjectionIntoExpand::class,
            ],
            $debugLogs[0]->record->attributes->get('optimizer_rules'),
        );
    }
}
