<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Provider\Memory;

use DateTimeImmutable;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Tests\Mother\InstrumentationScopeMother;
use Flow\Telemetry\Tests\Mother\LogEntryMother;
use Flow\Telemetry\Tests\Mother\MetricMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tests\Mother\SpanMother;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class MemoryExporterTest extends TestCase
{
    public function test_constructor_rejects_non_positive_max_entries(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('MemoryExporter maxEntriesPerSignal must be a positive integer, got 0');

        new MemoryExporter(0);
    }

    public function test_export_caps_logs_to_max_entries_dropping_oldest(): void
    {
        $exporter = new MemoryExporter(2);
        $entry1 = LogEntryMother::create('Log 1', Severity::INFO);
        $entry2 = LogEntryMother::create('Log 2', Severity::WARN);
        $entry3 = LogEntryMother::create('Log 3', Severity::ERROR);

        $exporter->export(Signals::logs([$entry1, $entry2]));
        $exporter->export(Signals::logs([$entry3]));

        static::assertCount(2, $exporter->logs());
        static::assertSame($entry2, $exporter->logs()[0]);
        static::assertSame($entry3, $exporter->logs()[1]);
    }

    public function test_export_caps_metrics_to_max_entries_dropping_oldest(): void
    {
        $exporter = new MemoryExporter(1);
        $first = MetricMother::counter('first', 1);
        $second = MetricMother::counter('second', 2);

        $exporter->export(Signals::metrics([$first]));
        $exporter->export(Signals::metrics([$second]));

        static::assertCount(1, $exporter->metrics());
        static::assertSame($second, $exporter->metrics()[0]);
    }

    public function test_export_caps_spans_to_max_entries_dropping_oldest(): void
    {
        $exporter = new MemoryExporter(2);
        $span1 = SpanMother::withName('span-1');
        $span2 = SpanMother::withName('span-2');
        $span3 = SpanMother::withName('span-3');

        $exporter->export(Signals::traces([$span1, $span2, $span3]));

        static::assertCount(2, $exporter->spans());
        static::assertSame($span2, $exporter->spans()[0]);
        static::assertSame($span3, $exporter->spans()[1]);
    }

    public function test_export_without_cap_keeps_every_entry(): void
    {
        $exporter = new MemoryExporter();

        for ($i = 0; $i < 100; $i++) {
            $exporter->export(Signals::traces([SpanMother::withName('span-' . $i)]));
        }

        static::assertCount(100, $exporter->spans());
    }

    public function test_export_empty_logs_does_not_modify_state(): void
    {
        $exporter = new MemoryExporter();

        static::assertTrue($exporter->export(Signals::logs([])));
        static::assertSame([], $exporter->logs());
    }

    public function test_export_empty_metrics_does_not_modify_state(): void
    {
        $exporter = new MemoryExporter();

        static::assertTrue($exporter->export(Signals::metrics([])));
        static::assertSame([], $exporter->metrics());
    }

    public function test_export_empty_traces_does_not_modify_state(): void
    {
        $exporter = new MemoryExporter();

        static::assertTrue($exporter->export(Signals::traces([])));
        static::assertSame([], $exporter->spans());
    }

    public function test_export_multiple_log_batches_appends(): void
    {
        $exporter = new MemoryExporter();
        $entry1 = LogEntryMother::create('Log 1', Severity::INFO);
        $entry2 = LogEntryMother::create('Log 2', Severity::WARN);
        $entry3 = LogEntryMother::create('Log 3', Severity::ERROR);

        $exporter->export(Signals::logs([$entry1, $entry2]));
        $exporter->export(Signals::logs([$entry3]));

        static::assertCount(3, $exporter->logs());
        static::assertSame($entry1, $exporter->logs()[0]);
        static::assertSame($entry2, $exporter->logs()[1]);
        static::assertSame($entry3, $exporter->logs()[2]);
    }

    public function test_export_multiple_metric_batches_appends(): void
    {
        $exporter = new MemoryExporter();
        $metric = new Metric(
            name: 'test.metric',
            type: MetricType::COUNTER,
            value: 1,
            attributes: Attributes::empty(),
            timestamp: new DateTimeImmutable(),
            resource: ResourceMother::default(),
            scope: InstrumentationScopeMother::default(),
        );

        $exporter->export(Signals::metrics([$metric]));
        $exporter->export(Signals::metrics([$metric]));

        static::assertCount(2, $exporter->metrics());
    }

    public function test_export_multiple_trace_batches_appends(): void
    {
        $exporter = new MemoryExporter();
        $span1 = SpanMother::withName('span-1');
        $span2 = SpanMother::withName('span-2');
        $span3 = SpanMother::withName('span-3');

        $exporter->export(Signals::traces([$span1, $span2]));
        $exporter->export(Signals::traces([$span3]));

        static::assertCount(3, $exporter->spans());
        static::assertSame($span1, $exporter->spans()[0]);
        static::assertSame($span2, $exporter->spans()[1]);
        static::assertSame($span3, $exporter->spans()[2]);
    }

    public function test_export_signals_are_independent(): void
    {
        $exporter = new MemoryExporter();
        $exporter->export(Signals::logs([LogEntryMother::create('Log', Severity::INFO)]));
        $exporter->export(Signals::traces([SpanMother::withName('span')]));

        static::assertCount(1, $exporter->logs());
        static::assertCount(0, $exporter->metrics());
        static::assertCount(1, $exporter->spans());
    }

    public function test_implements_exporter(): void
    {
        static::assertInstanceOf(Exporter::class, new MemoryExporter());
    }

    public function test_reset_clears_all_signals(): void
    {
        $exporter = new MemoryExporter();
        $exporter->export(Signals::logs([LogEntryMother::create('Log', Severity::INFO)]));
        $exporter->export(Signals::traces([SpanMother::withName('span')]));

        $exporter->reset();

        static::assertSame([], $exporter->logs());
        static::assertSame([], $exporter->metrics());
        static::assertSame([], $exporter->spans());
    }

    public function test_shutdown_is_noop(): void
    {
        $exporter = new MemoryExporter();

        $exporter->shutdown();

        $this->addToAssertionCount(1);
    }
}
