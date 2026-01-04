<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Provider\Memory;

use Flow\Telemetry\Provider\Memory\MemorySpanExporter;
use Flow\Telemetry\Tests\Mother\SpanMother;
use Flow\Telemetry\Tracer\SpanExporter;
use Flow\Telemetry\Transport\VoidTransport;
use PHPUnit\Framework\TestCase;

final class MemorySpanExporterTest extends TestCase
{
    public function test_export_empty_spans_returns_true() : void
    {
        $exporter = new MemorySpanExporter();

        self::assertTrue($exporter->export([]));
        self::assertSame([], $exporter->spans());
    }

    public function test_export_multiple_spans() : void
    {
        $exporter = new MemorySpanExporter();
        $span1 = SpanMother::withName('span-1');
        $span2 = SpanMother::withName('span-2');
        $span3 = SpanMother::withName('span-3');

        $exporter->export([$span1, $span2]);
        $exporter->export([$span3]);

        self::assertCount(3, $exporter->spans());
        self::assertSame($span1, $exporter->spans()[0]);
        self::assertSame($span2, $exporter->spans()[1]);
        self::assertSame($span3, $exporter->spans()[2]);
    }

    public function test_export_single_span() : void
    {
        $exporter = new MemorySpanExporter();
        $span = SpanMother::withName('test-span');

        $result = $exporter->export([$span]);

        self::assertTrue($result);
        self::assertCount(1, $exporter->spans());
        self::assertSame($span, $exporter->spans()[0]);
    }

    public function test_implements_span_exporter() : void
    {
        self::assertInstanceOf(SpanExporter::class, new MemorySpanExporter());
    }

    public function test_reset_clears_all_spans() : void
    {
        $exporter = new MemorySpanExporter();
        $exporter->export([SpanMother::withName('span-1'), SpanMother::withName('span-2')]);

        self::assertCount(2, $exporter->spans());

        $exporter->reset();

        self::assertSame([], $exporter->spans());
    }

    public function test_transports_returns_void_transport() : void
    {
        $exporter = new MemorySpanExporter();
        $transports = $exporter->transports();

        self::assertCount(1, $transports);
        self::assertInstanceOf(VoidTransport::class, $transports[0]);
    }
}
