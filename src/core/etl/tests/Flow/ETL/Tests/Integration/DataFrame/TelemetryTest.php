<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use DateTimeImmutable;
use Flow\ETL\Tests\FlowTestCase;
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
use Flow\Telemetry\Tracer\TracerProvider;
use Psr\Clock\ClockInterface;

use function count;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\telemetry_options;
use function Flow\ETL\DSL\to_array;
use function str_contains;
use function str_ends_with;

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

        $counterMetrics = $metricProcessor->metricsWithName('rows_processed');
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
            static::assertArrayHasKey('loader.class', $span->attributes());
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

        static::assertArrayHasKey('rows.total', $attributes);
        static::assertSame(5, $attributes['rows.total']);
        static::assertArrayHasKey('memory.min.mb', $attributes);
        static::assertArrayHasKey('memory.max.mb', $attributes);
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
            static::assertArrayHasKey('transformer.class', $span->attributes());
        }
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
