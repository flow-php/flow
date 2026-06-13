<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Provider\Composite;

use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Provider\Composite\CompositeExporter;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Tests\Mother\ExporterSpy;
use Flow\Telemetry\Tests\Mother\LogEntryMother;
use Flow\Telemetry\Tests\Mother\MetricMother;
use Flow\Telemetry\Tests\Mother\SpanMother;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(CompositeExporter::class)]
final class CompositeExporterTest extends TestCase
{
    public function test_implements_exporter(): void
    {
        static::assertInstanceOf(Exporter::class, new CompositeExporter([]));
    }

    public function test_forwards_logs_batch_to_every_exporter(): void
    {
        $first = new ExporterSpy();
        $second = new ExporterSpy();
        $signal = Signals::logs([LogEntryMother::create('Log', Severity::INFO)]);

        (new CompositeExporter([$first, $second]))->export($signal);

        static::assertSame([$signal], $first->exported());
        static::assertSame([$signal], $second->exported());
    }

    public function test_forwards_metrics_batch_to_every_exporter(): void
    {
        $first = new ExporterSpy();
        $second = new ExporterSpy();
        $signal = Signals::metrics([MetricMother::counter('test.metric', 1)]);

        (new CompositeExporter([$first, $second]))->export($signal);

        static::assertSame([$signal], $first->exported());
        static::assertSame([$signal], $second->exported());
    }

    public function test_forwards_traces_batch_to_every_exporter(): void
    {
        $first = new ExporterSpy();
        $second = new ExporterSpy();
        $signal = Signals::traces([SpanMother::withName('span')]);

        (new CompositeExporter([$first, $second]))->export($signal);

        static::assertSame([$signal], $first->exported());
        static::assertSame([$signal], $second->exported());
    }

    public function test_export_returns_false_when_any_exporter_returns_false(): void
    {
        $exporter = new CompositeExporter([new ExporterSpy(true), new ExporterSpy(false)]);

        static::assertFalse($exporter->export(Signals::traces([SpanMother::withName('span')])));
    }

    public function test_export_returns_true_when_all_exporters_return_true(): void
    {
        $exporter = new CompositeExporter([new ExporterSpy(true), new ExporterSpy(true)]);

        static::assertTrue($exporter->export(Signals::traces([SpanMother::withName('span')])));
    }

    public function test_export_does_not_short_circuit_on_failure(): void
    {
        $failing = new ExporterSpy(false);
        $trailing = new ExporterSpy(true);

        (new CompositeExporter([$failing, $trailing]))->export(Signals::traces([SpanMother::withName('span')]));

        static::assertSame(1, $trailing->exportedCount());
    }

    public function test_shutdown_shuts_down_every_exporter(): void
    {
        $first = new ExporterSpy();
        $second = new ExporterSpy();

        (new CompositeExporter([$first, $second]))->shutdown();

        static::assertSame(1, $first->shutdownCount());
        static::assertSame(1, $second->shutdownCount());
    }

    public function test_empty_exporter_list_export_is_noop_returning_true(): void
    {
        static::assertTrue((new CompositeExporter([]))->export(Signals::traces([SpanMother::withName('span')])));
    }

    public function test_empty_exporter_list_shutdown_is_noop(): void
    {
        (new CompositeExporter([]))->shutdown();

        $this->addToAssertionCount(1);
    }

    public function test_captures_into_memory_while_other_exporter_still_receives_batch(): void
    {
        $store = new MemoryExporter();
        $fake = new ExporterSpy();
        $span = SpanMother::withName('span');

        (new CompositeExporter([$fake, $store]))->export(Signals::traces([$span]));

        static::assertSame([$span], $store->spans());
        static::assertSame(1, $fake->exportedCount());
    }
}
