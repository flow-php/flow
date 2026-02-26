<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Provider\Console;

use function Flow\Telemetry\DSL\{console_log_options, console_log_options_minimal};
use Flow\Telemetry\Context\{SpanId, TraceId};
use Flow\Telemetry\Logger\{LogExporter, Severity};
use Flow\Telemetry\Provider\Console\ConsoleLogExporter;
use Flow\Telemetry\Tests\Mother\LogEntryMother;
use Flow\Telemetry\Tests\SnapshotTestTrait;
use Flow\Telemetry\Tracer\SpanContext;

use PHPUnit\Framework\TestCase;

final class ConsoleLogExporterTest extends TestCase
{
    use SnapshotTestTrait;

    public function test_export_empty_logs_returns_true() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleLogExporter(colors: false, outputStream: $stream);

        self::assertTrue($exporter->export([]));
    }

    public function test_implements_log_exporter() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        self::assertInstanceOf(LogExporter::class, new ConsoleLogExporter(colors: false, outputStream: $stream));
    }

    public function test_log_output_with_default_options() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleLogExporter(colors: false, outputStream: $stream);

        $entry = LogEntryMother::deterministic('User logged in', Severity::INFO, ['user_id' => 42]);

        $exporter->export([$entry]);

        \rewind($stream);
        $output = \stream_get_contents($stream);

        $this->assertMatchesSnapshot($output, __DIR__ . '/../../../Fixtures/Console/log_default_options.txt');
    }

    public function test_log_output_with_minimal_options() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleLogExporter(colors: false, outputStream: $stream, options: console_log_options_minimal());

        $entry = LogEntryMother::deterministic('User logged in', Severity::INFO);

        $exporter->export([$entry]);

        \rewind($stream);
        $output = \stream_get_contents($stream);

        $this->assertMatchesSnapshot($output, __DIR__ . '/../../../Fixtures/Console/log_minimal_options.txt');
    }

    public function test_log_with_scope() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleLogExporter(colors: false, outputStream: $stream, options: console_log_options()->withInstrumentationScope(true));

        $entry = LogEntryMother::deterministic('Database query executed', Severity::DEBUG);

        $exporter->export([$entry]);

        \rewind($stream);
        $output = \stream_get_contents($stream);

        $this->assertMatchesSnapshot($output, __DIR__ . '/../../../Fixtures/Console/log_with_scope.txt');
    }

    public function test_log_with_trace_context() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleLogExporter(colors: false, outputStream: $stream);

        $spanContext = SpanContext::create(
            TraceId::fromHex('0123456789abcdef0123456789abcdef'),
            SpanId::fromHex('0123456789abcdef'),
        );

        $entry = LogEntryMother::deterministic('Processing request', Severity::INFO, [], $spanContext);

        $exporter->export([$entry]);

        \rewind($stream);
        $output = \stream_get_contents($stream);

        $this->assertMatchesSnapshot($output, __DIR__ . '/../../../Fixtures/Console/log_with_trace_context.txt');
    }
}
