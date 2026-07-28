<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Config\Telemetry;

use DateTimeImmutable;
use Flow\ETL\Config\Telemetry\TelemetryContext;
use Flow\ETL\Config\Telemetry\TelemetryOptions;
use Flow\ETL\Loader\StreamLoader;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\LimitTransformer;
use Flow\ETL\Transformer\UntilTransformer;
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
use Psr\Clock\ClockInterface;
use RuntimeException;

use function array_map;
use function count;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\telemetry_options;
use function Flow\ETL\DSL\to_array;
use function Flow\ETL\DSL\to_stream;

final class TelemetryContextTest extends FlowTestCase
{
    public function test_dataframe_batch_processed_drains_abandoned_spans(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        $context->telemetryContext->dataFrameStarted($context->flowContext);

        $outer = new LimitTransformer(10);
        $inner = new UntilTransformer(ref('id')->lessThan(lit(5)));

        $context->telemetryContext->transformationStarted($outer);
        $context->telemetryContext->transformationStarted($inner);
        $context->telemetryContext->transformationCompleted($inner);

        $context->telemetryContext->dataFrameBatchProcessed(rows(row(int_entry('id', 1))), $context->flowContext);

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

    public function test_dataframe_batch_processed_tracks_rows_and_memory(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $clock = $this->createFrozenClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $telemetryContext = new TelemetryContext(
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            telemetry_options(collect_metrics: true),
        );

        $config = config_builder()->withTelemetry($telemetry, telemetry_options(collect_metrics: true))->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $rows = rows(row(int_entry('id', 1)), row(int_entry('id', 2)), row(int_entry('id', 3)));

        $telemetryContext->dataFrameBatchProcessed($rows, $context);

        $telemetryContext->dataFrameCompleted($context);
        $telemetry->flush();

        $metrics = $metricProcessor->metrics();
        static::assertNotEmpty($metrics);

        $counterMetrics = $metricProcessor->metricsWithName('flow.etl.rows.processed');
        static::assertNotEmpty($counterMetrics, 'Counter metrics should be collected');
        static::assertSame(3, $counterMetrics[0]->value);
    }

    public function test_dataframe_completed_drains_abandoned_spans(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        $context->telemetryContext->dataFrameStarted($context->flowContext);

        $outer = new LimitTransformer(10);
        $inner = new UntilTransformer(ref('id')->lessThan(lit(5)));

        $context->telemetryContext->transformationStarted($outer);
        $context->telemetryContext->transformationStarted($inner);
        $context->telemetryContext->transformationCompleted($inner);

        $context->telemetryContext->dataFrameCompleted($context->flowContext);

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
        $clock = $this->createFrozenClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $telemetryContext = new TelemetryContext(
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            new TelemetryOptions(),
        );

        $config = config_builder()->withTelemetry($telemetry)->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $rows = rows(row(int_entry('id', 1)), row(int_entry('id', 2)));

        $telemetryContext->dataFrameBatchProcessed($rows, $context);
        $telemetryContext->dataFrameCompleted($context);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $span = $spans[0];
        static::assertSame('DataFrame flow_dataframe', $span->name());
        // OTEL spec: instrumentation leaves the status Unset on success.
        static::assertNull($span->status());

        $attributes = $span->attributes();
        static::assertArrayHasKey('flow.etl.rows.total', $attributes);
        static::assertSame(2, $attributes['flow.etl.rows.total']);
        static::assertArrayHasKey('flow.etl.memory.min', $attributes);
        static::assertArrayHasKey('flow.etl.memory.max', $attributes);

        $debugLogs = $logProcessor->entriesWithSeverity(Severity::DEBUG);
        static::assertGreaterThanOrEqual(2, count($debugLogs));
    }

    public function test_dataframe_failed_drains_abandoned_spans(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        $context->telemetryContext->dataFrameStarted($context->flowContext);

        $outer = new LimitTransformer(10);
        $inner = new UntilTransformer(ref('id')->lessThan(lit(5)));

        $context->telemetryContext->transformationStarted($outer);
        $context->telemetryContext->transformationStarted($inner);
        $context->telemetryContext->transformationCompleted($inner);

        $context->telemetryContext->dataFrameFailed($context->flowContext, new RuntimeException('boom'));

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
        $clock = $this->createFrozenClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $telemetryContext = new TelemetryContext(
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            new TelemetryOptions(),
        );

        $config = config_builder()->withTelemetry($telemetry)->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $rows = rows(row(int_entry('id', 1)));
        $telemetryContext->dataFrameBatchProcessed($rows, $context);

        $exception = new RuntimeException('Processing failed due to invalid data');
        $telemetryContext->dataFrameFailed($context, $exception);

        $errorLogs = $logProcessor->entriesWithSeverity(Severity::ERROR);
        static::assertCount(1, $errorLogs);
        static::assertStringContainsString('Data frame processing failed', $errorLogs[0]->record->body);

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

    public function test_dataframe_started_creates_span_and_logs_debug_message(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $clock = $this->createFrozenClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $telemetryContext = new TelemetryContext(
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            new TelemetryOptions(),
        );

        $config = config_builder()->withTelemetry($telemetry)->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $startedSpans = $spanProcessor->startedSpans();
        static::assertCount(1, $startedSpans);
        static::assertSame('DataFrame flow_dataframe', $startedSpans[0]->name());

        $debugLogs = $logProcessor->entriesWithSeverity(Severity::DEBUG);
        static::assertCount(1, $debugLogs);
        static::assertStringContainsString('Data frame processing started', $debugLogs[0]->record->body);
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

    public function test_loading_completed_finalizes_span_with_ok_status(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $clock = $this->createFrozenClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $telemetryContext = new TelemetryContext(
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            telemetry_options(trace_loading: true),
        );

        $config = config_builder()->withTelemetry($telemetry, telemetry_options(trace_loading: true))->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $loader = new StreamLoader('php://memory');

        $telemetryContext->loadingStarted($loader);
        $telemetryContext->loadingCompleted($loader);

        $endedSpans = $spanProcessor->endedSpans();

        static::assertCount(1, $endedSpans);
        static::assertSame('StreamLoader', $endedSpans[0]->name());
        static::assertSame(StreamLoader::class, $endedSpans[0]->attributes()['flow.etl.loader.class']);
        // OTEL spec: instrumentation leaves the status Unset on success.
        static::assertNull($endedSpans[0]->status());
    }

    public function test_loading_completed_is_noop_when_tracing_disabled(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_loading: false));

        $context->telemetryContext->dataFrameStarted($context->flowContext);

        $loader = to_stream('php://memory');

        $context->telemetryContext->loadingStarted($loader);
        $context->telemetryContext->loadingCompleted($loader);

        static::assertEmpty($context->spans->endedSpans());
    }

    public function test_loading_completed_without_started_is_noop(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_loading: true));

        $context->telemetryContext->dataFrameStarted($context->flowContext);
        $context->telemetryContext->loadingCompleted(to_stream('php://memory'));

        static::assertEmpty($context->spans->endedSpans());
    }

    public function test_loading_failed_logs_error_and_sets_span_status(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $clock = $this->createFrozenClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $telemetryContext = new TelemetryContext(
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            telemetry_options(trace_loading: true),
        );

        $config = config_builder()->withTelemetry($telemetry, telemetry_options(trace_loading: true))->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $loader = new StreamLoader('php://memory');
        $exception = new RuntimeException('Loading failed due to disk error');

        $telemetryContext->loadingStarted($loader);
        $telemetryContext->loadingFailed($loader, $exception);

        $errorLogs = $logProcessor->entriesWithSeverity(Severity::ERROR);
        static::assertCount(1, $errorLogs);
        static::assertStringContainsString('Loading failed', $errorLogs[0]->record->body);

        $endedSpans = $spanProcessor->endedSpans();
        static::assertCount(1, $endedSpans);
        $status = $endedSpans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('Loading failed due to disk error', $status->description);
        static::assertSame(RuntimeException::class, $endedSpans[0]->attributes()['error.type']);
    }

    public function test_loading_failed_pops_the_innermost_span(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_loading: true));

        $context->telemetryContext->dataFrameStarted($context->flowContext);

        $output = [];
        $outer = to_stream('php://memory');
        $inner = to_array($output);

        $context->telemetryContext->loadingStarted($outer);
        $context->telemetryContext->loadingStarted($inner);
        $context->telemetryContext->loadingFailed($inner, new RuntimeException('Loading failed'));
        $context->telemetryContext->loadingCompleted($outer);

        $endedSpans = $context->spans->endedSpans();

        static::assertSame(
            ['ArrayLoader', 'StreamLoader'],
            array_map(static fn(Span $span): string => $span->name(), $endedSpans),
        );
        static::assertTrue($endedSpans[0]->status()?->isError());
        static::assertNull($endedSpans[1]->status());
    }

    public function test_loading_started_creates_span_when_trace_loading_enabled(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $clock = $this->createFrozenClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $telemetryContext = new TelemetryContext(
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            telemetry_options(trace_loading: true),
        );

        $config = config_builder()->withTelemetry($telemetry, telemetry_options(trace_loading: true))->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $loader = new StreamLoader('php://memory');
        $telemetryContext->loadingStarted($loader);

        $startedSpans = $spanProcessor->startedSpans();
        static::assertCount(2, $startedSpans);
        static::assertSame('StreamLoader', $startedSpans[1]->name());
        static::assertSame(StreamLoader::class, $startedSpans[1]->attributes()['flow.etl.loader.class']);
    }

    public function test_loading_started_does_not_create_span_when_trace_loading_disabled(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $clock = $this->createFrozenClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $telemetryContext = new TelemetryContext(
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            telemetry_options(trace_loading: false),
        );

        $config = config_builder()->withTelemetry($telemetry, telemetry_options(trace_loading: false))->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $loader = new StreamLoader('php://memory');
        $telemetryContext->loadingStarted($loader);

        $startedSpans = $spanProcessor->startedSpans();
        static::assertCount(1, $startedSpans);
        static::assertSame('DataFrame flow_dataframe', $startedSpans[0]->name());
    }

    public function test_metrics_collected_when_collect_metrics_enabled(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $clock = $this->createFrozenClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $telemetryContext = new TelemetryContext(
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            telemetry_options(collect_metrics: true),
        );

        $config = config_builder()->withTelemetry($telemetry, telemetry_options(collect_metrics: true))->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $rows = rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
        $telemetryContext->dataFrameBatchProcessed($rows, $context);
        $telemetryContext->dataFrameCompleted($context);
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
        $clock = $this->createFrozenClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $telemetryContext = new TelemetryContext(
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            telemetry_options(collect_metrics: true),
        );

        $config = config_builder()
            ->name('my_custom_dataframe')
            ->withTelemetry($telemetry, telemetry_options(collect_metrics: true))
            ->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $rows = rows(row(int_entry('id', 1)), row(int_entry('id', 2)), row(int_entry('id', 3)));
        $telemetryContext->dataFrameBatchProcessed($rows, $context);
        $telemetryContext->dataFrameCompleted($context);
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
        $clock = $this->createFrozenClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $telemetryContext = new TelemetryContext(
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            telemetry_options(collect_metrics: false),
        );

        $config = config_builder()->withTelemetry($telemetry, telemetry_options(collect_metrics: false))->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $rows = rows(row(int_entry('id', 1)));
        $telemetryContext->dataFrameBatchProcessed($rows, $context);
        $telemetryContext->dataFrameCompleted($context);
        $telemetry->flush();

        static::assertEmpty($metricProcessor->metrics(), 'No metrics should be collected when disabled');
    }

    public function test_nested_loadings_complete_both_spans_once(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_loading: true));

        $context->telemetryContext->dataFrameStarted($context->flowContext);

        $output = [];
        $outer = to_stream('php://memory');
        $inner = to_array($output);

        $context->telemetryContext->loadingStarted($outer);
        $context->telemetryContext->loadingStarted($inner);
        $context->telemetryContext->loadingCompleted($inner, ['nesting' => 'inner']);
        $context->telemetryContext->loadingCompleted($outer, ['nesting' => 'outer']);

        $endedSpans = $context->spans->endedSpans();

        static::assertCount(2, $endedSpans);
        static::assertSame('ArrayLoader', $endedSpans[0]->name());
        static::assertSame('inner', $endedSpans[0]->attributes()['nesting']);
        static::assertSame('StreamLoader', $endedSpans[1]->name());
        static::assertSame('outer', $endedSpans[1]->attributes()['nesting']);
    }

    public function test_nested_transformations_complete_both_spans_once(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        $context->telemetryContext->dataFrameStarted($context->flowContext);

        $outer = new LimitTransformer(10);
        $inner = new UntilTransformer(ref('id')->lessThan(lit(5)));

        $context->telemetryContext->transformationStarted($outer);
        $context->telemetryContext->transformationStarted($inner);
        $context->telemetryContext->transformationCompleted($inner, ['nesting' => 'inner']);
        $context->telemetryContext->transformationCompleted($outer, ['nesting' => 'outer']);

        $endedSpans = $context->spans->endedSpans();

        static::assertCount(2, $endedSpans);
        static::assertSame('UntilTransformer', $endedSpans[0]->name());
        static::assertSame('inner', $endedSpans[0]->attributes()['nesting']);
        static::assertSame('LimitTransformer', $endedSpans[1]->name());
        static::assertSame('outer', $endedSpans[1]->attributes()['nesting']);
    }

    public function test_transformation_completed_finalizes_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $clock = $this->createFrozenClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $telemetryContext = new TelemetryContext(
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            telemetry_options(trace_transformations: true),
        );

        $config = config_builder()->withTelemetry($telemetry, telemetry_options(trace_transformations: true))->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $transformer = new LimitTransformer(10);

        $telemetryContext->transformationStarted($transformer);
        $telemetryContext->transformationCompleted($transformer);

        $endedSpans = $spanProcessor->endedSpans();

        static::assertCount(1, $endedSpans);
        static::assertSame('LimitTransformer', $endedSpans[0]->name());
        static::assertSame(LimitTransformer::class, $endedSpans[0]->attributes()['flow.etl.transformer.class']);
        // OTEL spec: instrumentation leaves the status Unset on success.
        static::assertNull($endedSpans[0]->status());
    }

    public function test_transformation_completed_is_noop_when_tracing_disabled(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: false));

        $context->telemetryContext->dataFrameStarted($context->flowContext);

        $transformer = new LimitTransformer(10);

        $context->telemetryContext->transformationStarted($transformer);
        $context->telemetryContext->transformationCompleted($transformer);

        static::assertEmpty($context->spans->endedSpans());
    }

    public function test_transformation_completed_without_started_is_noop(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        $context->telemetryContext->dataFrameStarted($context->flowContext);
        $context->telemetryContext->transformationCompleted(new LimitTransformer(10));

        static::assertEmpty($context->spans->endedSpans());
    }

    public function test_transformation_failed_logs_error_and_sets_span_status(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $clock = $this->createFrozenClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $telemetryContext = new TelemetryContext(
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            telemetry_options(trace_transformations: true),
        );

        $config = config_builder()->withTelemetry($telemetry, telemetry_options(trace_transformations: true))->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $transformer = new LimitTransformer(10);
        $exception = new RuntimeException('Transformation failed');

        $telemetryContext->transformationStarted($transformer);
        $telemetryContext->transformationFailed($transformer, $exception);

        $errorLogs = $logProcessor->entriesWithSeverity(Severity::ERROR);
        static::assertCount(1, $errorLogs);
        static::assertStringContainsString('Transformation failed', $errorLogs[0]->record->body);

        $endedSpans = $spanProcessor->endedSpans();
        static::assertCount(1, $endedSpans);
        $status = $endedSpans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('Transformation failed', $status->description);
        static::assertSame(RuntimeException::class, $endedSpans[0]->attributes()['error.type']);
    }

    public function test_transformation_failed_pops_the_innermost_span(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        $context->telemetryContext->dataFrameStarted($context->flowContext);

        $outer = new LimitTransformer(10);
        $inner = new UntilTransformer(ref('id')->lessThan(lit(5)));

        $context->telemetryContext->transformationStarted($outer);
        $context->telemetryContext->transformationStarted($inner);
        $context->telemetryContext->transformationFailed($inner, new RuntimeException('Transformation failed'));
        $context->telemetryContext->transformationCompleted($outer);

        $endedSpans = $context->spans->endedSpans();

        static::assertSame(
            ['UntilTransformer', 'LimitTransformer'],
            array_map(static fn(Span $span): string => $span->name(), $endedSpans),
        );
        static::assertTrue($endedSpans[0]->status()?->isError());
        static::assertNull($endedSpans[1]->status());
    }

    public function test_transformation_started_creates_span_when_trace_transformations_enabled(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $clock = $this->createFrozenClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $telemetryContext = new TelemetryContext(
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            telemetry_options(trace_transformations: true),
        );

        $config = config_builder()->withTelemetry($telemetry, telemetry_options(trace_transformations: true))->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $transformer = new LimitTransformer(10);
        $telemetryContext->transformationStarted($transformer);

        $startedSpans = $spanProcessor->startedSpans();
        static::assertCount(2, $startedSpans);
        static::assertSame('LimitTransformer', $startedSpans[1]->name());
        static::assertSame(LimitTransformer::class, $startedSpans[1]->attributes()['flow.etl.transformer.class']);
    }

    public function test_transformation_started_does_not_create_span_when_trace_transformations_disabled(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $clock = $this->createFrozenClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $telemetryContext = new TelemetryContext(
            $telemetry->logger('flow-php'),
            $telemetry->tracer('flow-php'),
            $telemetry->meter('flow-php'),
            telemetry_options(trace_transformations: false),
        );

        $config = config_builder()->withTelemetry($telemetry, telemetry_options(trace_transformations: false))->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $transformer = new LimitTransformer(10);
        $telemetryContext->transformationStarted($transformer);

        $startedSpans = $spanProcessor->startedSpans();
        static::assertCount(1, $startedSpans);
        static::assertSame('DataFrame flow_dataframe', $startedSpans[0]->name());
    }

    private function createFrozenClock(DateTimeImmutable $now = new DateTimeImmutable()): ClockInterface
    {
        return new readonly class($now) implements ClockInterface {
            public function __construct(
                private DateTimeImmutable $now,
            ) {}

            public function now(): DateTimeImmutable
            {
                return $this->now;
            }
        };
    }
}
