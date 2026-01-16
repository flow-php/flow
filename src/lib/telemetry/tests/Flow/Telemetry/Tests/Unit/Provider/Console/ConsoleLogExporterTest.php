<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Provider\Console;

use Flow\Telemetry\Context\{SpanId, TraceId};
use Flow\Telemetry\Logger\{LogEntry, LogExporter, LogRecord, Severity};
use Flow\Telemetry\Provider\Console\ConsoleLogExporter;
use Flow\Telemetry\Tests\Mother\{InstrumentationScopeMother, ResourceMother};
use Flow\Telemetry\Tracer\SpanContext;
use PHPUnit\Framework\TestCase;

final class ConsoleLogExporterTest extends TestCase
{
    public function test_export_empty_logs_returns_true() : void
    {
        self::assertTrue($this->createExporter()->export([]));
    }

    public function test_export_outputs_log_body() : void
    {
        $stream = $this->createStream();
        $exporter = $this->createExporter($stream);

        $exporter->export([
            $this->createLogEntry('User logged in', Severity::INFO),
        ]);

        $output = $this->getOutput($stream);
        self::assertStringContainsString('User logged in', $output);
    }

    public function test_export_outputs_log_severity() : void
    {
        $stream = $this->createStream();
        $exporter = $this->createExporter($stream);

        $exporter->export([
            $this->createLogEntry('Test message', Severity::WARN),
        ]);

        $output = $this->getOutput($stream);
        self::assertStringContainsString('WARN', $output);
    }

    public function test_export_outputs_log_with_attributes() : void
    {
        $stream = $this->createStream();
        $exporter = $this->createExporter($stream);

        $exporter->export([
            $this->createLogEntry('User action', Severity::INFO, ['user_id' => 42]),
        ]);

        $output = $this->getOutput($stream);
        self::assertStringContainsString('user_id', $output);
        self::assertStringContainsString('42', $output);
    }

    public function test_export_outputs_log_with_span_context() : void
    {
        $stream = $this->createStream();
        $exporter = $this->createExporter($stream);

        $spanContext = SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
        );

        $exporter->export([
            new LogEntry(
                (new LogRecord())->setSeverity(Severity::INFO)->setBody('With trace'),
                ResourceMother::default(),
                InstrumentationScopeMother::default(),
                new \DateTimeImmutable(),
                $spanContext,
            ),
        ]);

        $output = $this->getOutput($stream);
        self::assertMatchesRegularExpression('/[a-f0-9]{8}\/[a-f0-9]{8}/', $output);
    }

    public function test_export_outputs_timestamp() : void
    {
        $stream = $this->createStream();
        $exporter = $this->createExporter($stream);

        $exporter->export([
            $this->createLogEntry('Test', Severity::INFO),
        ]);

        $output = $this->getOutput($stream);
        self::assertMatchesRegularExpression('/\d{4}-\d{2}-\d{2}/', $output);
    }

    public function test_implements_log_exporter() : void
    {
        self::assertInstanceOf(LogExporter::class, $this->createExporter());
    }

    /**
     * @param null|resource $stream
     */
    private function createExporter(mixed $stream = null) : ConsoleLogExporter
    {
        return new ConsoleLogExporter(colors: false, outputStream: $stream);
    }

    /**
     * @param array<string, array<bool|\DateTimeImmutable|float|int|string>|bool|\DateTimeImmutable|float|int|string> $attributes
     */
    private function createLogEntry(string $body, Severity $severity, array $attributes = []) : LogEntry
    {
        return new LogEntry(
            (new LogRecord())->setSeverity($severity)->setBody($body)->setAttributes($attributes),
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            new \DateTimeImmutable(),
        );
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
