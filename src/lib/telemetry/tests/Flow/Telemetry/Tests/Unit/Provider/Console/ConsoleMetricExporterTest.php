<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Provider\Console;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Meter\{Metric, MetricExporter, MetricType};
use Flow\Telemetry\Provider\Console\ConsoleMetricExporter;
use Flow\Telemetry\Tests\Mother\{InstrumentationScopeMother, ResourceMother};
use PHPUnit\Framework\TestCase;

final class ConsoleMetricExporterTest extends TestCase
{
    public function test_export_empty_metrics_returns_true() : void
    {
        self::assertTrue($this->createExporter()->export([]));
    }

    public function test_export_outputs_attributes() : void
    {
        $stream = $this->createStream();
        $exporter = $this->createExporter($stream);

        $exporter->export([
            new Metric(
                name: 'test.metric',
                type: MetricType::COUNTER,
                value: 1,
                attributes: Attributes::create(['source' => 'csv', 'pipeline' => 'main']),
                timestamp: new \DateTimeImmutable(),
                resource: ResourceMother::default(),
                scope: InstrumentationScopeMother::default(),
            ),
        ]);

        $output = $this->getOutput($stream);
        self::assertStringContainsString('source=csv', $output);
        self::assertStringContainsString('pipeline=main', $output);
    }

    public function test_export_outputs_counter_with_icon() : void
    {
        $stream = $this->createStream();
        $exporter = $this->createExporter($stream);

        $exporter->export([
            new Metric(
                name: 'http.requests',
                type: MetricType::COUNTER,
                value: 100,
                attributes: Attributes::empty(),
                timestamp: new \DateTimeImmutable(),
                resource: ResourceMother::default(),
                scope: InstrumentationScopeMother::default(),
            ),
        ]);

        $output = $this->getOutput($stream);
        self::assertStringContainsString('^', $output);
        self::assertStringContainsString('http.requests', $output);
    }

    public function test_export_outputs_gauge_with_icon() : void
    {
        $stream = $this->createStream();
        $exporter = $this->createExporter($stream);

        $exporter->export([
            new Metric(
                name: 'memory.usage',
                type: MetricType::GAUGE,
                value: 1024,
                attributes: Attributes::empty(),
                timestamp: new \DateTimeImmutable(),
                resource: ResourceMother::default(),
                scope: InstrumentationScopeMother::default(),
            ),
        ]);

        $output = $this->getOutput($stream);
        self::assertStringContainsString('o', $output);
        self::assertStringContainsString('memory.usage', $output);
    }

    public function test_export_outputs_histogram_with_icon() : void
    {
        $stream = $this->createStream();
        $exporter = $this->createExporter($stream);

        $exporter->export([
            new Metric(
                name: 'response.time',
                type: MetricType::HISTOGRAM,
                value: 50.5,
                attributes: Attributes::empty(),
                timestamp: new \DateTimeImmutable(),
                resource: ResourceMother::default(),
                scope: InstrumentationScopeMother::default(),
            ),
        ]);

        $output = $this->getOutput($stream);
        self::assertStringContainsString('#', $output);
        self::assertStringContainsString('response.time', $output);
    }

    public function test_export_outputs_metrics_header() : void
    {
        $stream = $this->createStream();
        $exporter = $this->createExporter($stream);

        $exporter->export([
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

        $output = $this->getOutput($stream);
        self::assertStringContainsString('METRICS', $output);
    }

    public function test_export_outputs_unit() : void
    {
        $stream = $this->createStream();
        $exporter = $this->createExporter($stream);

        $exporter->export([
            new Metric(
                name: 'memory.bytes',
                type: MetricType::GAUGE,
                value: 1024,
                attributes: Attributes::empty(),
                timestamp: new \DateTimeImmutable(),
                resource: ResourceMother::default(),
                scope: InstrumentationScopeMother::default(),
                unit: 'bytes',
            ),
        ]);

        $output = $this->getOutput($stream);
        self::assertStringContainsString('[bytes]', $output);
    }

    public function test_export_outputs_up_down_counter_with_icon() : void
    {
        $stream = $this->createStream();
        $exporter = $this->createExporter($stream);

        $exporter->export([
            new Metric(
                name: 'queue.size',
                type: MetricType::UP_DOWN_COUNTER,
                value: 5,
                attributes: Attributes::empty(),
                timestamp: new \DateTimeImmutable(),
                resource: ResourceMother::default(),
                scope: InstrumentationScopeMother::default(),
            ),
        ]);

        $output = $this->getOutput($stream);
        self::assertStringContainsString('~', $output);
        self::assertStringContainsString('queue.size', $output);
    }

    public function test_implements_metric_exporter() : void
    {
        self::assertInstanceOf(MetricExporter::class, $this->createExporter());
    }

    /**
     * @param null|resource $stream
     */
    private function createExporter(mixed $stream = null) : ConsoleMetricExporter
    {
        return new ConsoleMetricExporter(colors: false, outputStream: $stream);
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
