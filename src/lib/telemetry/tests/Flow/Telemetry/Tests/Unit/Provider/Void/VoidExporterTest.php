<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Provider\Void;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Meter\{Metric, MetricType};
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Tests\Mother\{InstrumentationScopeMother, LogEntryMother, ResourceMother, SpanMother};
use PHPUnit\Framework\TestCase;

final class VoidExporterTest extends TestCase
{
    public function test_export_empty_logs_returns_true() : void
    {
        self::assertTrue((new VoidExporter())->export(Signals::logs([])));
    }

    public function test_export_empty_metrics_returns_true() : void
    {
        self::assertTrue((new VoidExporter())->export(Signals::metrics([])));
    }

    public function test_export_empty_traces_returns_true() : void
    {
        self::assertTrue((new VoidExporter())->export(Signals::traces([])));
    }

    public function test_export_logs_batch_returns_true() : void
    {
        $batch = Signals::logs([
            LogEntryMother::create('Log 1', Severity::INFO),
            LogEntryMother::create('Log 2', Severity::WARN),
        ]);

        self::assertTrue((new VoidExporter())->export($batch));
    }

    public function test_export_metrics_batch_returns_true() : void
    {
        $batch = Signals::metrics([
            new Metric(
                name: 'test.metric',
                type: MetricType::COUNTER,
                value: 1,
                attributes: Attributes::empty(),
                timestamp: new \DateTimeImmutable(),
                resource: ResourceMother::default(),
                scope: InstrumentationScopeMother::default(),
            ),
        ]);

        self::assertTrue((new VoidExporter())->export($batch));
    }

    public function test_export_traces_batch_returns_true() : void
    {
        $batch = Signals::traces([
            SpanMother::withName('span-1'),
            SpanMother::withName('span-2'),
        ]);

        self::assertTrue((new VoidExporter())->export($batch));
    }

    public function test_implements_exporter() : void
    {
        self::assertInstanceOf(Exporter::class, new VoidExporter());
    }

    public function test_shutdown_is_noop() : void
    {
        $exporter = new VoidExporter();

        $exporter->shutdown();

        $this->addToAssertionCount(1);
    }
}
