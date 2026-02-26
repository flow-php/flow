<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Provider\Console;

use function Flow\Telemetry\DSL\{console_span_options, console_span_options_minimal};
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Context\{SpanId, TraceId};
use Flow\Telemetry\Provider\Console\ConsoleSpanExporter;
use Flow\Telemetry\Tests\Mother\SpanMother;
use Flow\Telemetry\Tests\SnapshotTestTrait;
use Flow\Telemetry\Tracer\{GenericEvent, SpanContext, SpanExporter, SpanKind, SpanLink, SpanStatus};

use PHPUnit\Framework\TestCase;

final class ConsoleSpanExporterTest extends TestCase
{
    use SnapshotTestTrait;

    public function test_export_empty_spans_returns_true() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleSpanExporter(colors: false, outputStream: $stream);

        self::assertTrue($exporter->export([]));
    }

    public function test_implements_span_exporter() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        self::assertInstanceOf(SpanExporter::class, new ConsoleSpanExporter(colors: false, outputStream: $stream));
    }

    public function test_span_output_with_default_options() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleSpanExporter(colors: false, outputStream: $stream);

        $span = SpanMother::deterministic('test-operation', SpanKind::SERVER)
            ->setAttribute('http.method', 'GET')
            ->setAttribute('http.url', '/api/users')
            ->setStatus(SpanStatus::ok())
            ->end(new \DateTimeImmutable('2024-01-15T10:30:00.150000+00:00'));

        $exporter->export([$span]);

        \rewind($stream);
        $output = \stream_get_contents($stream);

        $this->assertMatchesSnapshot($output, __DIR__ . '/../../../Fixtures/Console/span_default_options.txt');
    }

    public function test_span_output_with_minimal_options() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleSpanExporter(colors: false, outputStream: $stream, options: console_span_options_minimal());

        $span = SpanMother::deterministic('test-operation', SpanKind::SERVER)
            ->setAttribute('http.method', 'GET')
            ->setStatus(SpanStatus::ok())
            ->end(new \DateTimeImmutable('2024-01-15T10:30:00.150000+00:00'));

        $exporter->export([$span]);

        \rewind($stream);
        $output = \stream_get_contents($stream);

        $this->assertMatchesSnapshot($output, __DIR__ . '/../../../Fixtures/Console/span_minimal_options.txt');
    }

    public function test_span_with_dropped_counts() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleSpanExporter(colors: false, outputStream: $stream, options: console_span_options()->withDroppedCounts(true));

        $span = SpanMother::deterministic('test-operation', SpanKind::SERVER)
            ->setStatus(SpanStatus::ok())
            ->end(new \DateTimeImmutable('2024-01-15T10:30:00.150000+00:00'));

        $exporter->export([$span]);

        \rewind($stream);
        $output = \stream_get_contents($stream);

        $this->assertMatchesSnapshot($output, __DIR__ . '/../../../Fixtures/Console/span_with_dropped_counts.txt');
    }

    public function test_span_with_error_status_and_description() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleSpanExporter(colors: false, outputStream: $stream, options: console_span_options()->withStatusDescription(true));

        $span = SpanMother::deterministic('test-operation', SpanKind::SERVER)
            ->setStatus(SpanStatus::error('Connection refused'))
            ->end(new \DateTimeImmutable('2024-01-15T10:30:00.150000+00:00'));

        $exporter->export([$span]);

        \rewind($stream);
        $output = \stream_get_contents($stream);

        $this->assertMatchesSnapshot($output, __DIR__ . '/../../../Fixtures/Console/span_with_error_status.txt');
    }

    public function test_span_with_events() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleSpanExporter(colors: false, outputStream: $stream);

        $span = SpanMother::deterministic('test-operation', SpanKind::SERVER);
        $span->recordEvent(GenericEvent::create('query.start', new \DateTimeImmutable('2024-01-15T10:30:00.050000+00:00'), Attributes::empty()));
        $span->recordEvent(GenericEvent::create('query.end', new \DateTimeImmutable('2024-01-15T10:30:00.100000+00:00'), Attributes::create(['rows' => 42])));
        $span->setStatus(SpanStatus::ok())->end(new \DateTimeImmutable('2024-01-15T10:30:00.150000+00:00'));

        $exporter->export([$span]);

        \rewind($stream);
        $output = \stream_get_contents($stream);

        $this->assertMatchesSnapshot($output, __DIR__ . '/../../../Fixtures/Console/span_with_events.txt');
    }

    public function test_span_with_links() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleSpanExporter(colors: false, outputStream: $stream, options: console_span_options()->withLinks(true));

        $span = SpanMother::deterministic('test-operation', SpanKind::SERVER);

        $linkedContext1 = SpanContext::create(
            TraceId::fromHex('aaaabbbbccccddddaaaabbbbccccdddd'),
            SpanId::fromHex('1111222233334444'),
        );
        $linkedContext2 = SpanContext::create(
            TraceId::fromHex('eeeeffffaaaabbbbeeeeffffaaaabbbb'),
            SpanId::fromHex('5555666677778888'),
        );

        $span->addLink(SpanLink::create($linkedContext1));
        $span->addLink(SpanLink::create($linkedContext2));
        $span->setStatus(SpanStatus::ok())->end(new \DateTimeImmutable('2024-01-15T10:30:00.150000+00:00'));

        $exporter->export([$span]);

        \rewind($stream);
        $output = \stream_get_contents($stream);

        $this->assertMatchesSnapshot($output, __DIR__ . '/../../../Fixtures/Console/span_with_links.txt');
    }
}
