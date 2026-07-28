<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use DateTimeImmutable;
use Flow\ETL\Loader\RetryLoader;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\LimitTransformer;
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

use function array_filter;
use function count;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\limit;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\telemetry_options;
use function Flow\ETL\DSL\to_array;
use function Flow\ETL\DSL\to_transformation;
use function Flow\ETL\DSL\with_entry;
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
        $clock = $this->createFrozenClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $config = config_builder()->withTelemetry($telemetry, telemetry_options(collect_metrics: true))->build();

        df($config)->read(from_array([
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
        $clock = $this->createFrozenClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $config = config_builder()->withTelemetry($telemetry, telemetry_options(trace_loading: true))->build();

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
        $clock = $this->createFrozenClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $config = config_builder()->withTelemetry($telemetry)->build();

        df($config)->read(from_array([
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

    public function test_dataframe_run_logs_start_and_completion(): void
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

        $config = config_builder()->withTelemetry($telemetry)->build();

        df($config)->read(from_array([
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
        $clock = $this->createFrozenClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $config = config_builder()->withTelemetry($telemetry)->build();

        df($config)->read(from_array([
            ['id' => 1, 'name' => 'John'],
            ['id' => 2, 'name' => 'Jane'],
            ['id' => 3, 'name' => 'Doe'],
            ['id' => 4, 'name' => 'Smith'],
            ['id' => 5, 'name' => 'Brown'],
        ]))->run();

        $endedSpans = $spanProcessor->endedSpans();
        static::assertCount(1, $endedSpans);

        $dataFrameSpan = $endedSpans[0];
        $attributes = $dataFrameSpan->attributes();

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
        $clock = $this->createFrozenClock();
        $contextStorage = new MemoryContextStorage();

        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'flow-test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        $config = config_builder()->withTelemetry($telemetry, telemetry_options(trace_transformations: true))->build();

        df($config)
            ->read(from_array([
                ['id' => 1, 'name' => 'John'],
                ['id' => 2, 'name' => 'Jane'],
            ]))
            ->withEntry('upper_name', ref('name')->upper())
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

    public function test_duplicate_row_transformer_exports_nested_spans_once(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_transformations: true));

        $output = [];
        df($context->config)
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->duplicateRow(condition: lit(true), entries: with_entry('id', ref('id')->multiply(lit(-1))))
            ->write(to_array($output))
            ->run();

        $endedSpans = $context->spans->endedSpans();

        static::assertCount(2, array_filter(
            $endedSpans,
            static fn(Span $span): bool => $span->name() === 'DuplicateRowTransformer',
        ));
        static::assertCount(2, array_filter(
            $endedSpans,
            static fn(Span $span): bool => $span->name() === 'ScalarFunctionTransformer',
        ));
    }

    public function test_limit_reached_inside_transformer_loader_is_not_reported_as_failure(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_loading: true));

        $rows = [];

        for ($id = 1; $id <= 20; $id++) {
            $rows[] = ['id' => $id];
        }

        $output = [];
        df($context->config)
            ->read(from_array($rows))
            ->load(to_transformation(new LimitTransformer(10), to_array($output)))
            ->run();

        static::assertEmpty($context->logs->entriesContaining('Loading failed'));
        static::assertEmpty($context->logs->entriesContaining('Error during ETL segment execution.'));
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

    public function test_limit_reached_is_logged_without_exception_attribute(): void
    {
        $context = new MemoryTelemetryContext();

        $rows = [];

        for ($id = 1; $id <= 20; $id++) {
            $rows[] = ['id' => $id];
        }

        df($context->config)->read(from_array($rows))->limit(5)->run();

        $entries = $context->logs->entriesContaining('Limit reached');

        static::assertCount(1, $entries);
        static::assertFalse($entries[0]->record->attributes->has('limit_exception'));
        static::assertSame(5, $entries[0]->record->attributes->get('limit'));
    }

    public function test_retry_loader_exports_nested_spans_once(): void
    {
        $context = new MemoryTelemetryContext(telemetry_options(trace_loading: true));

        $output = [];
        df($context->config)
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->load(new RetryLoader(to_array($output)))
            ->run();

        $endedSpans = $context->spans->endedSpans();

        static::assertCount(2, array_filter(
            $endedSpans,
            static fn(Span $span): bool => $span->name() === 'RetryLoader',
        ));
        static::assertCount(2, array_filter(
            $endedSpans,
            static fn(Span $span): bool => $span->name() === 'ArrayLoader',
        ));
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

    public function test_transformation_loader_builds_one_nested_dataframe_span_per_run(): void
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

        static::assertCount(2, array_filter($context->spans->startedSpans(), $isDataFrameSpan));
        static::assertCount(2, array_filter($context->spans->endedSpans(), $isDataFrameSpan));
    }

    public function test_until_condition_is_logged_without_exception_attribute(): void
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

        $entries = $context->logs->entriesContaining('Limit reached');

        static::assertCount(1, $entries);
        static::assertFalse($entries[0]->record->attributes->has('limit_exception'));
        // UntilTransformer has no limit to report and throws LimitReachedException(0).
        static::assertSame(0, $entries[0]->record->attributes->get('limit'));
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
