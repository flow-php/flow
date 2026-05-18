<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Unit\Exporter;

use DateTimeImmutable;
use Flow\Bridge\Telemetry\OTLP\Exporter\OTLPExporter;
use Flow\Bridge\Telemetry\OTLP\Transport\Transport;
use Flow\Bridge\Telemetry\OTLP\Transport\TransportException;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\ErrorHandler\NullErrorHandler;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\LogRecord;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Tests\Mother\ErrorHandlerSpy;
use Flow\Telemetry\Tests\Mother\InstrumentationScopeMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tests\Mother\SpanMother;
use PHPUnit\Framework\TestCase;

final class OTLPExporterTest extends TestCase
{
    public function test_export_does_not_call_error_handler_on_empty_signal(): void
    {
        $transport = new RecordingTransport();
        $spy = new ErrorHandlerSpy();
        $exporter = new OTLPExporter($transport, $spy);

        static::assertTrue($exporter->export(Signals::logs([])));
        static::assertSame(0, $spy->count());
    }

    public function test_export_empty_logs_returns_true_without_calling_transport(): void
    {
        $transport = new RecordingTransport();
        $exporter = new OTLPExporter($transport);

        static::assertTrue($exporter->export(Signals::logs([])));
        static::assertSame(0, $transport->callCount());
    }

    public function test_export_empty_metrics_returns_true_without_calling_transport(): void
    {
        $transport = new RecordingTransport();
        $exporter = new OTLPExporter($transport);

        static::assertTrue($exporter->export(Signals::metrics([])));
        static::assertSame(0, $transport->callCount());
    }

    public function test_export_empty_traces_returns_true_without_calling_transport(): void
    {
        $transport = new RecordingTransport();
        $exporter = new OTLPExporter($transport);

        static::assertTrue($exporter->export(Signals::traces([])));
        static::assertSame(0, $transport->callCount());
    }

    public function test_export_logs_calls_transport(): void
    {
        $transport = new RecordingTransport();
        $exporter = new OTLPExporter($transport);
        $entry = new LogEntry(
            (new LogRecord())
                ->setSeverity(Severity::INFO)
                ->setBody('hello'),
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            new DateTimeImmutable(),
        );

        static::assertTrue($exporter->export(Signals::logs([$entry])));
        static::assertSame(1, $transport->callCount());
    }

    public function test_export_metrics_calls_transport(): void
    {
        $transport = new RecordingTransport();
        $exporter = new OTLPExporter($transport);
        $metric = new Metric(
            name: 'test.metric',
            type: MetricType::COUNTER,
            value: 1,
            attributes: Attributes::empty(),
            timestamp: new DateTimeImmutable(),
            resource: ResourceMother::default(),
            scope: InstrumentationScopeMother::default(),
        );

        static::assertTrue($exporter->export(Signals::metrics([$metric])));
        static::assertSame(1, $transport->callCount());
    }

    public function test_export_returns_false_when_transport_throws(): void
    {
        $transport = new ThrowingTransport();
        $exporter = new OTLPExporter($transport, new NullErrorHandler());

        static::assertFalse($exporter->export(Signals::traces([SpanMother::withName('span')])));
    }

    public function test_export_routes_transport_throwable_to_error_handler(): void
    {
        $transport = new ThrowingTransport();
        $spy = new ErrorHandlerSpy();
        $exporter = new OTLPExporter($transport, $spy);

        static::assertFalse($exporter->export(Signals::traces([SpanMother::withName('span')])));
        static::assertSame(1, $spy->count());
        $last = $spy->last();
        static::assertInstanceOf(TransportException::class, $last);
        static::assertSame('boom', $last->getMessage());
    }

    public function test_export_traces_calls_transport(): void
    {
        $transport = new RecordingTransport();
        $exporter = new OTLPExporter($transport);

        static::assertTrue($exporter->export(Signals::traces([SpanMother::withName('span')])));
        static::assertSame(1, $transport->callCount());
    }

    public function test_shutdown_delegates_to_transport(): void
    {
        $transport = new RecordingTransport();
        $exporter = new OTLPExporter($transport);

        $exporter->shutdown();

        static::assertTrue($transport->isShutdown());
    }

    public function test_shutdown_routes_transport_throwable_to_error_handler(): void
    {
        $transport = new ShutdownThrowingTransport();
        $spy = new ErrorHandlerSpy();
        $exporter = new OTLPExporter($transport, $spy);

        $exporter->shutdown();

        static::assertSame(1, $spy->count());
        $last = $spy->last();
        static::assertInstanceOf(TransportException::class, $last);
        static::assertSame('shutdown boom', $last->getMessage());
    }
}

final class RecordingTransport implements Transport
{
    private int $callCount = 0;

    private bool $isShutdown = false;

    public function callCount(): int
    {
        return $this->callCount;
    }

    public function isShutdown(): bool
    {
        return $this->isShutdown;
    }

    public function send(Signals $signal): void
    {
        $this->callCount++;
    }

    public function shutdown(): void
    {
        $this->isShutdown = true;
    }
}

final class ThrowingTransport implements Transport
{
    public function send(Signals $signal): void
    {
        throw new TransportException('boom');
    }

    public function shutdown(): void {}
}

final class ShutdownThrowingTransport implements Transport
{
    public function send(Signals $signal): void {}

    public function shutdown(): void
    {
        throw new TransportException('shutdown boom');
    }
}
