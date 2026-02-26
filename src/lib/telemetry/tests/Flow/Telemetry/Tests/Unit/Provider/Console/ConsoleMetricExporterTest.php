<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Provider\Console;

use function Flow\Telemetry\DSL\{console_metric_options, console_metric_options_minimal};
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Context\{SpanId, TraceId};
use Flow\Telemetry\Meter\{Exemplar, Metric, MetricExporter, MetricType};
use Flow\Telemetry\Provider\Console\ConsoleMetricExporter;
use Flow\Telemetry\Tests\Mother\{InstrumentationScopeMother, MetricMother, ResourceMother};
use Flow\Telemetry\Tests\SnapshotTestTrait;

use PHPUnit\Framework\TestCase;

final class ConsoleMetricExporterTest extends TestCase
{
    use SnapshotTestTrait;

    public function test_export_empty_metrics_returns_true() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleMetricExporter(colors: false, outputStream: $stream);

        self::assertTrue($exporter->export([]));
    }

    public function test_implements_metric_exporter() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        self::assertInstanceOf(MetricExporter::class, new ConsoleMetricExporter(colors: false, outputStream: $stream));
    }

    public function test_metric_output_with_default_options() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleMetricExporter(colors: false, outputStream: $stream);

        $metric = MetricMother::deterministicCounter('http.requests', 100, 'requests', 'Total HTTP requests');

        $exporter->export([$metric]);

        \rewind($stream);
        $output = \stream_get_contents($stream);

        $this->assertMatchesSnapshot($output, __DIR__ . '/../../../Fixtures/Console/metric_default_options.txt');
    }

    public function test_metric_output_with_minimal_options() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleMetricExporter(colors: false, outputStream: $stream, options: console_metric_options_minimal());

        $metric = MetricMother::deterministicCounter('http.requests', 100, 'requests');

        $exporter->export([$metric]);

        \rewind($stream);
        $output = \stream_get_contents($stream);

        $this->assertMatchesSnapshot($output, __DIR__ . '/../../../Fixtures/Console/metric_minimal_options.txt');
    }

    public function test_metric_with_all_exemplars() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleMetricExporter(colors: false, outputStream: $stream, options: console_metric_options()->withAllExemplars(true));

        $exemplar1 = new Exemplar(
            100,
            new \DateTimeImmutable('2024-01-15T10:30:00.000000+00:00'),
            TraceId::fromHex('aaaabbbbccccddddaaaabbbbccccdddd'),
            SpanId::fromHex('1111222233334444'),
            [],
        );
        $exemplar2 = new Exemplar(
            200,
            new \DateTimeImmutable('2024-01-15T10:30:00.000000+00:00'),
            TraceId::fromHex('eeeeffffaaaabbbbeeeeffffaaaabbbb'),
            SpanId::fromHex('5555666677778888'),
            [],
        );

        $metric = new Metric(
            name: 'http.requests',
            type: MetricType::COUNTER,
            value: 300,
            attributes: Attributes::empty(),
            timestamp: new \DateTimeImmutable('2024-01-15T10:30:00.000000+00:00'),
            resource: ResourceMother::full(),
            scope: InstrumentationScopeMother::default(),
            unit: 'requests',
            exemplars: [$exemplar1, $exemplar2],
        );

        $exporter->export([$metric]);

        \rewind($stream);
        $output = \stream_get_contents($stream);

        $this->assertMatchesSnapshot($output, __DIR__ . '/../../../Fixtures/Console/metric_with_all_exemplars.txt');
    }

    public function test_metric_with_description() : void
    {
        $stream = \fopen('php://memory', 'rwb');
        self::assertIsResource($stream);

        $exporter = new ConsoleMetricExporter(colors: false, outputStream: $stream, options: console_metric_options()->withDescription(true));

        $metric = MetricMother::deterministicGauge('memory.usage', 1024, 'bytes', 'Current memory usage');

        $exporter->export([$metric]);

        \rewind($stream);
        $output = \stream_get_contents($stream);

        $this->assertMatchesSnapshot($output, __DIR__ . '/../../../Fixtures/Console/metric_with_description.txt');
    }
}
