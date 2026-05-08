<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Unit\Exporter;

use Flow\Bridge\Telemetry\OTLP\Exporter\OTLPExporter;
use Flow\Bridge\Telemetry\OTLP\Transport\{Transport, TransportException};
use Flow\Telemetry\Attributes;
use Flow\Telemetry\ErrorHandler\NullErrorHandler;
use Flow\Telemetry\Logger\{LogEntry, LogRecord, Severity};
use Flow\Telemetry\Meter\{Metric, MetricType};
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Tests\Mother\{ErrorHandlerSpy, InstrumentationScopeMother, ResourceMother, SpanMother};
use PHPUnit\Framework\TestCase;

final class OTLPExporterTest extends TestCase
{
    public function test_export_does_not_call_error_handler_on_empty_signal() : void
    {
        $transport = new RecordingTransport();
        $spy = new ErrorHandlerSpy();
        $exporter = new OTLPExporter($transport, $spy);

        self::assertTrue($exporter->export(Signals::logs([])));
        self::assertSame(0, $spy->count());
    }

    public function test_export_empty_logs_returns_true_without_calling_transport() : void
    {
        $transport = new RecordingTransport();
        $exporter = new OTLPExporter($transport);

        self::assertTrue($exporter->export(Signals::logs([])));
        self::assertSame(0, $transport->callCount());
    }

    public function test_export_empty_metrics_returns_true_without_calling_transport() : void
    {
        $transport = new RecordingTransport();
        $exporter = new OTLPExporter($transport);

        self::assertTrue($exporter->export(Signals::metrics([])));
        self::assertSame(0, $transport->callCount());
    }

    public function test_export_empty_traces_returns_true_without_calling_transport() : void
    {
        $transport = new RecordingTransport();
        $exporter = new OTLPExporter($transport);

        self::assertTrue($exporter->export(Signals::traces([])));
        self::assertSame(0, $transport->callCount());
    }

    public function test_export_logs_calls_transport() : void
    {
        $transport = new RecordingTransport();
        $exporter = new OTLPExporter($transport);
        $entry = new LogEntry(
            (new LogRecord())->setSeverity(Severity::INFO)->setBody('hello'),
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            new \DateTimeImmutable(),
        );

        self::assertTrue($exporter->export(Signals::logs([$entry])));
        self::assertSame(1, $transport->callCount());
    }

    public function test_export_metrics_calls_transport() : void
    {
        $transport = new RecordingTransport();
        $exporter = new OTLPExporter($transport);
        $metric = new Metric(
            name: 'test.metric',
            type: MetricType::COUNTER,
            value: 1,
            attributes: Attributes::empty(),
            timestamp: new \DateTimeImmutable(),
            resource: ResourceMother::default(),
            scope: InstrumentationScopeMother::default(),
        );

        self::assertTrue($exporter->export(Signals::metrics([$metric])));
        self::assertSame(1, $transport->callCount());
    }

    public function test_export_returns_false_when_transport_throws() : void
    {
        $transport = new ThrowingTransport();
        $exporter = new OTLPExporter($transport, new NullErrorHandler());

        self::assertFalse($exporter->export(Signals::traces([SpanMother::withName('span')])));
    }

    public function test_export_routes_transport_throwable_to_error_handler() : void
    {
        $transport = new ThrowingTransport();
        $spy = new ErrorHandlerSpy();
        $exporter = new OTLPExporter($transport, $spy);

        self::assertFalse($exporter->export(Signals::traces([SpanMother::withName('span')])));
        self::assertSame(1, $spy->count());
        $last = $spy->last();
        self::assertInstanceOf(TransportException::class, $last);
        self::assertSame('boom', $last->getMessage());
    }

    public function test_export_traces_calls_transport() : void
    {
        $transport = new RecordingTransport();
        $exporter = new OTLPExporter($transport);

        self::assertTrue($exporter->export(Signals::traces([SpanMother::withName('span')])));
        self::assertSame(1, $transport->callCount());
    }

    public function test_shutdown_delegates_to_transport() : void
    {
        $transport = new RecordingTransport();
        $exporter = new OTLPExporter($transport);

        $exporter->shutdown();

        self::assertTrue($transport->isShutdown());
    }

    public function test_shutdown_routes_transport_throwable_to_error_handler() : void
    {
        $transport = new ShutdownThrowingTransport();
        $spy = new ErrorHandlerSpy();
        $exporter = new OTLPExporter($transport, $spy);

        $exporter->shutdown();

        self::assertSame(1, $spy->count());
        $last = $spy->last();
        self::assertInstanceOf(TransportException::class, $last);
        self::assertSame('shutdown boom', $last->getMessage());
    }
}

final class RecordingTransport implements Transport
{
    private int $callCount = 0;

    private bool $isShutdown = false;

    public function callCount() : int
    {
        return $this->callCount;
    }

    public function isShutdown() : bool
    {
        return $this->isShutdown;
    }

    public function send(Signals $signal) : void
    {
        $this->callCount++;
    }

    public function shutdown() : void
    {
        $this->isShutdown = true;
    }
}

final class ThrowingTransport implements Transport
{
    public function send(Signals $signal) : void
    {
        throw new TransportException('boom');
    }

    public function shutdown() : void
    {
    }
}

final class ShutdownThrowingTransport implements Transport
{
    public function send(Signals $signal) : void
    {
    }

    public function shutdown() : void
    {
        throw new TransportException('shutdown boom');
    }
}
