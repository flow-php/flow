<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Provider\Console;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Meter\{Metric, MetricType};
use Flow\Telemetry\Provider\Console\ConsoleExporter;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Tests\Mother\{InstrumentationScopeMother, LogEntryMother, ResourceMother, SpanMother};
use Flow\Telemetry\Tracer\{SpanKind, SpanStatus};
use PHPUnit\Framework\TestCase;

final class ConsoleExporterTest extends TestCase
{
    public function test_export_empty_logs_writes_nothing() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleExporter(colors: false, outputStream: $stream);

        self::assertTrue($exporter->export(Signals::logs([])));

        \rewind($stream);
        self::assertSame('', \stream_get_contents($stream));
    }

    public function test_export_empty_metrics_writes_nothing() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleExporter(colors: false, outputStream: $stream);

        self::assertTrue($exporter->export(Signals::metrics([])));

        \rewind($stream);
        self::assertSame('', \stream_get_contents($stream));
    }

    public function test_export_empty_traces_writes_nothing() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleExporter(colors: false, outputStream: $stream);

        self::assertTrue($exporter->export(Signals::traces([])));

        \rewind($stream);
        self::assertSame('', \stream_get_contents($stream));
    }

    public function test_export_logs_writes_logs_section() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleExporter(colors: false, outputStream: $stream);
        $exporter->export(Signals::logs([LogEntryMother::create('Hello world', Severity::INFO)]));

        \rewind($stream);
        $output = (string) \stream_get_contents($stream);

        self::assertStringContainsString('LOGS', $output);
        self::assertStringContainsString('Hello world', $output);
    }

    public function test_export_metrics_writes_metrics_section() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $metric = new Metric(
            name: 'requests.total',
            type: MetricType::COUNTER,
            value: 42,
            attributes: Attributes::empty(),
            timestamp: new \DateTimeImmutable(),
            resource: ResourceMother::default(),
            scope: InstrumentationScopeMother::default(),
        );

        $exporter = new ConsoleExporter(colors: false, outputStream: $stream);
        $exporter->export(Signals::metrics([$metric]));

        \rewind($stream);
        $output = (string) \stream_get_contents($stream);

        self::assertStringContainsString('METRICS', $output);
        self::assertStringContainsString('requests.total', $output);
    }

    public function test_export_traces_writes_span_section() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $span = SpanMother::deterministic('test-operation', SpanKind::SERVER)
            ->setStatus(SpanStatus::ok())
            ->end(new \DateTimeImmutable('2024-01-15T10:30:00.150000+00:00'));

        $exporter = new ConsoleExporter(colors: false, outputStream: $stream);
        $exporter->export(Signals::traces([$span]));

        \rewind($stream);
        $output = (string) \stream_get_contents($stream);

        self::assertStringContainsString('SPAN: test-operation', $output);
    }

    public function test_implements_exporter() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        self::assertInstanceOf(Exporter::class, new ConsoleExporter(colors: false, outputStream: $stream));
    }

    public function test_shutdown_is_noop() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleExporter(colors: false, outputStream: $stream);
        $exporter->shutdown();

        $this->addToAssertionCount(1);
    }
}
