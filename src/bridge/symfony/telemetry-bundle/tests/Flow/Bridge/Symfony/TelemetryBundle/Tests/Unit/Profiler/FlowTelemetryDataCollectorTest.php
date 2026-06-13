<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Profiler;

use DateTimeImmutable;
use Flow\Bridge\Symfony\TelemetryBundle\Profiler\FlowTelemetryDataCollector;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Tests\Mother\LogEntryMother;
use Flow\Telemetry\Tests\Mother\MetricMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tests\Mother\SpanMother;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

use function array_map;
use function Flow\Telemetry\DSL\telemetry;
use function sprintf;

#[CoversClass(FlowTelemetryDataCollector::class)]
final class FlowTelemetryDataCollectorTest extends TestCase
{
    private const string TRACE_ID = '0123456789abcdef0123456789abcdef';

    private const string ROOT_ID = '1111111111111111';

    private const string CHILD_ID = '2222222222222222';

    private const string GRANDCHILD_ID = '3333333333333333';

    public function test_get_name_is_flow_telemetry(): void
    {
        $store = new MemoryExporter();

        static::assertSame('flow_telemetry', $this->collector($store)->getName());
    }

    public function test_late_collect_orders_span_rows_by_start_time(): void
    {
        $store = $this->storeWithSpanTree();
        $collector = $this->collector($store);

        $collector->lateCollect();

        $names = array_map(static fn(array $row): string => $row['name'], $collector->getSpans());
        static::assertSame(['root', 'child', 'grandchild'], $names);
    }

    public function test_late_collect_computes_depth_from_parent_chain(): void
    {
        $store = $this->storeWithSpanTree();
        $collector = $this->collector($store);

        $collector->lateCollect();

        $depthByName = [];

        foreach ($collector->getSpans() as $row) {
            $depthByName[$row['name']] = $row['depth'];
        }

        static::assertSame(['root' => 0, 'child' => 1, 'grandchild' => 2], $depthByName);
    }

    public function test_late_collect_offsets_are_relative_to_earliest_span(): void
    {
        $store = $this->storeWithSpanTree();
        $collector = $this->collector($store);

        $collector->lateCollect();

        $offsetByName = [];

        foreach ($collector->getSpans() as $row) {
            $offsetByName[$row['name']] = $row['offsetMs'];
        }

        static::assertSame(0.0, $offsetByName['root']);
        static::assertSame(5.0, $offsetByName['child']);
        static::assertSame(8.0, $offsetByName['grandchild']);
    }

    public function test_late_collect_computes_timeline_window_from_latest_end(): void
    {
        $store = $this->storeWithSpanTree();
        $collector = $this->collector($store);

        $collector->lateCollect();

        static::assertSame(50.0, $collector->getTimelineDurationMs());
    }

    public function test_late_collect_totals_span_durations(): void
    {
        $store = $this->storeWithSpanTree();
        $collector = $this->collector($store);

        $collector->lateCollect();

        static::assertSame(3, $collector->getSpanCount());
        static::assertSame(69.0, $collector->getTotalDurationMs());
    }

    public function test_late_collect_normalizes_metric_rows(): void
    {
        $store = new MemoryExporter();
        $store->export(Signals::metrics([MetricMother::counter('app.requests', 5, 'count')]));
        $collector = $this->collector($store);

        $collector->lateCollect();

        static::assertSame(1, $collector->getMetricCount());
        $metric = $collector->getMetrics()[0];
        static::assertSame('app.requests', $metric['name']);
        static::assertSame(5, $metric['value']);
        static::assertSame('count', $metric['unit']);
    }

    public function test_late_collect_normalizes_log_rows(): void
    {
        $store = new MemoryExporter();
        $store->export(Signals::logs([LogEntryMother::create('something happened', Severity::WARN)]));
        $collector = $this->collector($store);

        $collector->lateCollect();

        static::assertSame(1, $collector->getLogCount());
        $log = $collector->getLogs()[0];
        static::assertSame('something happened', $log['message']);
        static::assertSame('WARN', $log['severity']);
        static::assertSame(13, $log['severityCode']);
        static::assertFalse($log['hasAttributes']);
    }

    public function test_late_collect_flags_logs_with_attributes(): void
    {
        $store = new MemoryExporter();
        $store->export(Signals::logs([LogEntryMother::create('with ctx', Severity::INFO, ['user_id' => 7])]));
        $collector = $this->collector($store);

        $collector->lateCollect();

        static::assertTrue($collector->getLogs()[0]['hasAttributes']);
    }

    public function test_logs_are_empty_when_none_captured(): void
    {
        $collector = $this->collector(new MemoryExporter());

        $collector->lateCollect();

        static::assertSame(0, $collector->getLogCount());
        static::assertSame([], $collector->getLogs());
    }

    public function test_signal_count_sums_spans_metrics_and_logs(): void
    {
        $store = $this->storeWithSpanTree();
        $store->export(Signals::metrics([MetricMother::counter('app.requests', 1)]));
        $store->export(Signals::logs([LogEntryMother::create('hello', Severity::INFO)]));
        $collector = $this->collector($store);

        $collector->lateCollect();

        static::assertSame(3, $collector->getSpanCount());
        static::assertSame(1, $collector->getMetricCount());
        static::assertSame(1, $collector->getLogCount());
        static::assertSame(5, $collector->getSignalCount());
    }

    public function test_reset_clears_data_and_store(): void
    {
        $store = $this->storeWithSpanTree();
        $collector = $this->collector($store);
        $collector->lateCollect();

        $collector->reset();

        static::assertSame([], $collector->getSpans());
        static::assertSame(0, $collector->getSpanCount());
        static::assertSame([], $store->spans());
    }

    public function test_empty_store_yields_no_rows(): void
    {
        $collector = $this->collector(new MemoryExporter());

        $collector->lateCollect();

        static::assertSame([], $collector->getSpans());
        static::assertSame(0, $collector->getSpanCount());
        static::assertSame(0.0, $collector->getTimelineDurationMs());
    }

    private function collector(MemoryExporter $store): FlowTelemetryDataCollector
    {
        return new FlowTelemetryDataCollector(telemetry(ResourceMother::default()), $store);
    }

    private function storeWithSpanTree(): MemoryExporter
    {
        $traceId = TraceId::fromHex(self::TRACE_ID);
        $base = new DateTimeImmutable('2024-01-01T00:00:00.000000+00:00');

        $root = $this->endedSpan('root', $traceId, SpanId::fromHex(self::ROOT_ID), null, SpanKind::SERVER, $base, 50.0);
        $child = $this->endedSpan(
            'child',
            $traceId,
            SpanId::fromHex(self::CHILD_ID),
            SpanId::fromHex(self::ROOT_ID),
            SpanKind::INTERNAL,
            $base->modify('+5000 microseconds'),
            15.0,
        );
        $grandchild = $this->endedSpan(
            'grandchild',
            $traceId,
            SpanId::fromHex(self::GRANDCHILD_ID),
            SpanId::fromHex(self::CHILD_ID),
            SpanKind::CLIENT,
            $base->modify('+8000 microseconds'),
            4.0,
        );

        $store = new MemoryExporter();
        // Inserted out of start order to prove the collector sorts by start time.
        $store->export(Signals::traces([$grandchild, $root, $child]));

        return $store;
    }

    private function endedSpan(
        string $name,
        TraceId $traceId,
        SpanId $spanId,
        ?SpanId $parentSpanId,
        SpanKind $kind,
        DateTimeImmutable $startTime,
        float $durationMs,
    ): Span {
        $span = SpanMother::create($name, $traceId, $spanId, $parentSpanId, $kind, $startTime);

        return $span->end($startTime->modify(sprintf('+%d microseconds', (int) ($durationMs * 1000))));
    }
}
