<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Provider\Conditional;

use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Provider\Conditional\ConditionalExporter;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Tests\Mother\ExporterSpy;
use Flow\Telemetry\Tests\Mother\SpanMother;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(ConditionalExporter::class)]
final class ConditionalExporterTest extends TestCase
{
    public function test_implements_exporter(): void
    {
        static::assertInstanceOf(Exporter::class, new ConditionalExporter(true, new ExporterSpy()));
    }

    public function test_forwards_batch_when_enabled(): void
    {
        $inner = new ExporterSpy();
        $signal = Signals::traces([SpanMother::withName('span')]);

        (new ConditionalExporter(true, $inner))->export($signal);

        static::assertSame([$signal], $inner->exported());
    }

    public function test_returns_inner_result_when_enabled(): void
    {
        static::assertFalse((new ConditionalExporter(
            true,
            new ExporterSpy(false),
        ))->export(Signals::traces([SpanMother::withName('span')])));
    }

    public function test_drops_batch_without_calling_inner_when_disabled(): void
    {
        $inner = new ExporterSpy();

        (new ConditionalExporter(false, $inner))->export(Signals::traces([SpanMother::withName('span')]));

        static::assertSame(0, $inner->exportedCount());
    }

    public function test_reports_success_when_disabled(): void
    {
        static::assertTrue((new ConditionalExporter(
            false,
            new ExporterSpy(false),
        ))->export(Signals::traces([SpanMother::withName('span')])));
    }

    public function test_shutdown_delegates_to_inner_when_enabled(): void
    {
        $inner = new ExporterSpy();

        (new ConditionalExporter(true, $inner))->shutdown();

        static::assertSame(1, $inner->shutdownCount());
    }

    public function test_shutdown_delegates_to_inner_when_disabled(): void
    {
        $inner = new ExporterSpy();

        (new ConditionalExporter(false, $inner))->shutdown();

        static::assertSame(1, $inner->shutdownCount());
    }
}
