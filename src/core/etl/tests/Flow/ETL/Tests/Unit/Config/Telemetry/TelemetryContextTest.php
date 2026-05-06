<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Config\Telemetry;

use function Flow\ETL\DSL\{config_builder, flow_context, telemetry_options};
use function Flow\ETL\DSL\{int_entry, row, rows};
use Flow\ETL\Config\Telemetry\{TelemetryContext, TelemetryOptions};
use Flow\ETL\Loader\StreamLoader;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\LimitTransformer;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\{LoggerProvider, Severity};
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Memory\{MemoryLogProcessor, MemoryMetricProcessor, MemorySpanProcessor};
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\{Resource, Telemetry};
use Flow\Telemetry\Tracer\TracerProvider;
use Psr\Clock\ClockInterface;

final class TelemetryContextTest extends FlowTestCase
{
    public function test_dataframe_batch_processed_tracks_rows_and_memory() : void
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
            ->withTelemetry($telemetry, telemetry_options(collect_metrics: true))
            ->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );

        $telemetryContext->dataFrameBatchProcessed($rows, $context);

        $telemetryContext->dataFrameCompleted($context);
        $telemetry->flush();

        $metrics = $metricProcessor->metrics();
        self::assertNotEmpty($metrics);

        $counterMetrics = $metricProcessor->metricsWithName('rows_processed');
        self::assertNotEmpty($counterMetrics, 'Counter metrics should be collected');
        self::assertSame(3, $counterMetrics[0]->value);
    }

    public function test_dataframe_completed_finalizes_span_with_statistics() : void
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

        $config = config_builder()
            ->withTelemetry($telemetry)
            ->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
        );

        $telemetryContext->dataFrameBatchProcessed($rows, $context);
        $telemetryContext->dataFrameCompleted($context);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('DataFrame flow_dataframe', $span->name());
        self::assertNotNull($span->status());
        self::assertTrue($span->status()->isOk());

        $attributes = $span->attributes();
        self::assertArrayHasKey('rows.total', $attributes);
        self::assertSame(2, $attributes['rows.total']);
        self::assertArrayHasKey('memory.min.mb', $attributes);
        self::assertArrayHasKey('memory.max.mb', $attributes);

        $debugLogs = $logProcessor->entriesWithSeverity(Severity::DEBUG);
        self::assertGreaterThanOrEqual(2, \count($debugLogs));
    }

    public function test_dataframe_failed_logs_error_and_sets_span_status() : void
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

        $config = config_builder()
            ->withTelemetry($telemetry)
            ->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $rows = rows(row(int_entry('id', 1)));
        $telemetryContext->dataFrameBatchProcessed($rows, $context);

        $exception = new \RuntimeException('Processing failed due to invalid data');
        $telemetryContext->dataFrameFailed($context, $exception);

        $errorLogs = $logProcessor->entriesWithSeverity(Severity::ERROR);
        self::assertCount(1, $errorLogs);
        self::assertStringContainsString('Data frame processing failed', $errorLogs[0]->record->body);

        $endedSpans = $spanProcessor->endedSpans();
        self::assertCount(1, $endedSpans);
        self::assertNotNull($endedSpans[0]->status());
        self::assertTrue($endedSpans[0]->status()->isError());
        self::assertSame('Processing failed due to invalid data', $endedSpans[0]->status()->description);

        $attributes = $endedSpans[0]->attributes();
        self::assertArrayHasKey('rows.total', $attributes);
        self::assertSame(1, $attributes['rows.total']);
    }

    public function test_dataframe_started_creates_span_and_logs_debug_message() : void
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

        $config = config_builder()
            ->withTelemetry($telemetry)
            ->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $startedSpans = $spanProcessor->startedSpans();
        self::assertCount(1, $startedSpans);
        self::assertSame('DataFrame flow_dataframe', $startedSpans[0]->name());

        $debugLogs = $logProcessor->entriesWithSeverity(Severity::DEBUG);
        self::assertCount(1, $debugLogs);
        self::assertStringContainsString('Data frame processing started', $debugLogs[0]->record->body);
    }

    public function test_loading_completed_finalizes_span_with_ok_status() : void
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

        $config = config_builder()
            ->withTelemetry($telemetry, telemetry_options(trace_loading: true))
            ->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $loader = new StreamLoader('php://memory');

        $telemetryContext->loadingStarted($loader);
        $telemetryContext->loadingCompleted($loader);

        $endedSpans = $spanProcessor->endedSpans();

        self::assertCount(1, $endedSpans);
        self::assertSame('StreamLoader', $endedSpans[0]->name());
        self::assertSame(StreamLoader::class, $endedSpans[0]->attributes()['loader.class']);
        self::assertNotNull($endedSpans[0]->status());
        self::assertTrue($endedSpans[0]->status()->isOk());
    }

    public function test_loading_failed_logs_error_and_sets_span_status() : void
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

        $config = config_builder()
            ->withTelemetry($telemetry, telemetry_options(trace_loading: true))
            ->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $loader = new StreamLoader('php://memory');
        $exception = new \RuntimeException('Loading failed due to disk error');

        $telemetryContext->loadingStarted($loader);
        $telemetryContext->loadingFailed($loader, $exception);

        $errorLogs = $logProcessor->entriesWithSeverity(Severity::ERROR);
        self::assertCount(1, $errorLogs);
        self::assertStringContainsString('Loading failed', $errorLogs[0]->record->body);

        $endedSpans = $spanProcessor->endedSpans();
        self::assertCount(1, $endedSpans);
        self::assertNotNull($endedSpans[0]->status());
        self::assertTrue($endedSpans[0]->status()->isError());
        self::assertSame('Loading failed due to disk error', $endedSpans[0]->status()->description);
    }

    public function test_loading_started_creates_span_when_trace_loading_enabled() : void
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

        $config = config_builder()
            ->withTelemetry($telemetry, telemetry_options(trace_loading: true))
            ->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $loader = new StreamLoader('php://memory');
        $telemetryContext->loadingStarted($loader);

        $startedSpans = $spanProcessor->startedSpans();
        self::assertCount(2, $startedSpans);
        self::assertSame('StreamLoader', $startedSpans[1]->name());
        self::assertSame(StreamLoader::class, $startedSpans[1]->attributes()['loader.class']);
    }

    public function test_loading_started_does_not_create_span_when_trace_loading_disabled() : void
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

        $config = config_builder()
            ->withTelemetry($telemetry, telemetry_options(trace_loading: false))
            ->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $loader = new StreamLoader('php://memory');
        $telemetryContext->loadingStarted($loader);

        $startedSpans = $spanProcessor->startedSpans();
        self::assertCount(1, $startedSpans);
        self::assertSame('DataFrame flow_dataframe', $startedSpans[0]->name());
    }

    public function test_metrics_collected_when_collect_metrics_enabled() : void
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
            ->withTelemetry($telemetry, telemetry_options(collect_metrics: true))
            ->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
        );
        $telemetryContext->dataFrameBatchProcessed($rows, $context);
        $telemetryContext->dataFrameCompleted($context);
        $telemetry->flush();

        $counterMetrics = $metricProcessor->metricsWithName('rows_processed');
        $throughputMetrics = $metricProcessor->metricsWithName('rows_throughput');

        self::assertNotEmpty($counterMetrics, 'Counter should be created when metrics enabled');
        self::assertNotEmpty($throughputMetrics, 'Throughput should be created when metrics enabled');
    }

    public function test_metrics_include_dataframe_name_attribute() : void
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

        $rows = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2)),
            row(int_entry('id', 3)),
        );
        $telemetryContext->dataFrameBatchProcessed($rows, $context);
        $telemetryContext->dataFrameCompleted($context);
        $telemetry->flush();

        $counterMetrics = $metricProcessor->metricsWithName('rows_processed');
        $throughputMetrics = $metricProcessor->metricsWithName('rows_throughput');

        self::assertCount(1, $counterMetrics);
        self::assertSame(3, $counterMetrics[0]->value);
        self::assertSame('my_custom_dataframe', $counterMetrics[0]->attributes->get('dataframe.name'));

        self::assertCount(1, $throughputMetrics);
        self::assertSame('my_custom_dataframe', $throughputMetrics[0]->attributes->get('dataframe.name'));
    }

    public function test_metrics_not_collected_when_collect_metrics_disabled() : void
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

        $config = config_builder()
            ->withTelemetry($telemetry, telemetry_options(collect_metrics: false))
            ->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $rows = rows(
            row(int_entry('id', 1)),
        );
        $telemetryContext->dataFrameBatchProcessed($rows, $context);
        $telemetryContext->dataFrameCompleted($context);
        $telemetry->flush();

        self::assertEmpty($metricProcessor->metrics(), 'No metrics should be collected when disabled');
    }

    public function test_transformation_completed_finalizes_span() : void
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

        $config = config_builder()
            ->withTelemetry($telemetry, telemetry_options(trace_transformations: true))
            ->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $transformer = new LimitTransformer(10);

        $telemetryContext->transformationStarted($transformer);
        $telemetryContext->transformationCompleted($transformer);

        $endedSpans = $spanProcessor->endedSpans();

        self::assertCount(1, $endedSpans);
        self::assertSame('LimitTransformer', $endedSpans[0]->name());
        self::assertSame(LimitTransformer::class, $endedSpans[0]->attributes()['transformer.class']);
        self::assertNotNull($endedSpans[0]->status());
        self::assertTrue($endedSpans[0]->status()->isOk());
    }

    public function test_transformation_failed_logs_error_and_sets_span_status() : void
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

        $config = config_builder()
            ->withTelemetry($telemetry, telemetry_options(trace_transformations: true))
            ->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $transformer = new LimitTransformer(10);
        $exception = new \RuntimeException('Transformation failed');

        $telemetryContext->transformationStarted($transformer);
        $telemetryContext->transformationFailed($transformer, $exception);

        $errorLogs = $logProcessor->entriesWithSeverity(Severity::ERROR);
        self::assertCount(1, $errorLogs);
        self::assertStringContainsString('Transformation failed', $errorLogs[0]->record->body);

        $endedSpans = $spanProcessor->endedSpans();
        self::assertCount(1, $endedSpans);
        self::assertNotNull($endedSpans[0]->status());
        self::assertTrue($endedSpans[0]->status()->isError());
        self::assertSame('Transformation failed', $endedSpans[0]->status()->description);
    }

    public function test_transformation_started_creates_span_when_trace_transformations_enabled() : void
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

        $config = config_builder()
            ->withTelemetry($telemetry, telemetry_options(trace_transformations: true))
            ->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $transformer = new LimitTransformer(10);
        $telemetryContext->transformationStarted($transformer);

        $startedSpans = $spanProcessor->startedSpans();
        self::assertCount(2, $startedSpans);
        self::assertSame('LimitTransformer', $startedSpans[1]->name());
        self::assertSame(LimitTransformer::class, $startedSpans[1]->attributes()['transformer.class']);
    }

    public function test_transformation_started_does_not_create_span_when_trace_transformations_disabled() : void
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

        $config = config_builder()
            ->withTelemetry($telemetry, telemetry_options(trace_transformations: false))
            ->build();

        $context = flow_context($config);

        $telemetryContext->dataFrameStarted($context);

        $transformer = new LimitTransformer(10);
        $telemetryContext->transformationStarted($transformer);

        $startedSpans = $spanProcessor->startedSpans();
        self::assertCount(1, $startedSpans);
        self::assertSame('DataFrame flow_dataframe', $startedSpans[0]->name());
    }

    private function createFrozenClock(\DateTimeImmutable $now = new \DateTimeImmutable()) : ClockInterface
    {
        return new readonly class($now) implements ClockInterface {
            public function __construct(private \DateTimeImmutable $now)
            {
            }

            public function now() : \DateTimeImmutable
            {
                return $this->now;
            }
        };
    }
}
