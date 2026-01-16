<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Provider\Console;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Provider\Console\ConsoleSpanExporter;
use Flow\Telemetry\Tests\Mother\SpanMother;
use Flow\Telemetry\Tracer\{GenericEvent, SpanExporter, SpanKind, SpanStatus};
use PHPUnit\Framework\TestCase;

final class ConsoleSpanExporterTest extends TestCase
{
    public function test_export_empty_spans_returns_true() : void
    {
        self::assertTrue($this->createExporter()->export([]));
    }

    public function test_export_outputs_span_with_attributes() : void
    {
        $stream = $this->createStream();
        $exporter = $this->createExporter($stream);

        $span = SpanMother::create('test-span')
            ->setAttribute('http.method', 'GET')
            ->setAttribute('http.url', '/api/users')
            ->end();

        $exporter->export([$span]);

        $output = $this->getOutput($stream);
        self::assertStringContainsString('Attributes:', $output);
        self::assertStringContainsString('http.method', $output);
        self::assertStringContainsString('GET', $output);
    }

    public function test_export_outputs_span_with_events() : void
    {
        $stream = $this->createStream();
        $exporter = $this->createExporter($stream);

        $span = SpanMother::create('test-span');
        $span->recordEvent(new GenericEvent('query.start', new \DateTimeImmutable(), Attributes::empty()));
        $span->end();

        $exporter->export([$span]);

        $output = $this->getOutput($stream);
        self::assertStringContainsString('Events (1):', $output);
        self::assertStringContainsString('query.start', $output);
    }

    public function test_export_outputs_span_with_status_error() : void
    {
        $stream = $this->createStream();
        $exporter = $this->createExporter($stream);

        $span = SpanMother::create('error-span')
            ->setStatus(SpanStatus::error('Something went wrong'))
            ->end();

        $exporter->export([$span]);

        $output = $this->getOutput($stream);
        self::assertStringContainsString('ERROR', $output);
    }

    public function test_export_outputs_span_with_status_ok() : void
    {
        $stream = $this->createStream();
        $exporter = $this->createExporter($stream);

        $span = SpanMother::create('ok-span')
            ->setStatus(SpanStatus::ok())
            ->end();

        $exporter->export([$span]);

        $output = $this->getOutput($stream);
        self::assertStringContainsString('OK', $output);
    }

    public function test_export_writes_span_header() : void
    {
        $stream = $this->createStream();
        $exporter = $this->createExporter($stream);

        $exporter->export([SpanMother::withName('test-span')]);

        $output = $this->getOutput($stream);
        self::assertStringContainsString('SPAN:', $output);
        self::assertStringContainsString('test-span', $output);
    }

    public function test_export_writes_span_kind() : void
    {
        $stream = $this->createStream();
        $exporter = $this->createExporter($stream);

        $span = SpanMother::create('server-span', kind: SpanKind::SERVER);
        $exporter->export([$span]);

        $output = $this->getOutput($stream);
        self::assertStringContainsString('SERVER', $output);
    }

    public function test_export_writes_trace_info() : void
    {
        $stream = $this->createStream();
        $exporter = $this->createExporter($stream);

        $exporter->export([SpanMother::withName('test-span')]);

        $output = $this->getOutput($stream);
        self::assertStringContainsString('Trace:', $output);
        self::assertStringContainsString('Span:', $output);
    }

    public function test_implements_span_exporter() : void
    {
        self::assertInstanceOf(SpanExporter::class, $this->createExporter());
    }

    /**
     * @param null|resource $stream
     */
    private function createExporter(mixed $stream = null) : ConsoleSpanExporter
    {
        return new ConsoleSpanExporter(colors: false, outputStream: $stream);
    }

    /**
     * @return resource
     */
    private function createStream()
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        return $stream;
    }

    /**
     * @param resource $stream
     */
    private function getOutput($stream) : string
    {
        \rewind($stream);

        return \stream_get_contents($stream);
    }
}
