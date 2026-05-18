<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Integration\Meter;

use DateTimeImmutable;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;
use Psr\Clock\ClockInterface;

use function array_filter;

final class MetricsIntegrationTest extends TestCase
{
    private ClockInterface $clock;

    private Resource $resource;

    protected function setUp(): void
    {
        $this->clock = $this->createMock(ClockInterface::class);
        $this->clock->method('now')->willReturn(new DateTimeImmutable('2024-01-01 12:00:00.123456'));
        $this->resource = ResourceMother::default();
    }

    public function test_complete_metrics_workflow(): void
    {
        $processor = $this->createProcessor();
        $provider = new MeterProvider($processor, $this->clock);
        $meter = $provider->meter($this->resource, 'my-service', '1.0.0');

        $requestsCounter = $meter->createCounter('http.requests.total');
        $requestsCounter->add(1, ['method' => 'GET', 'status' => 200]);
        $requestsCounter->add(1, ['method' => 'POST', 'status' => 201]);
        $requestsCounter->add(1, ['method' => 'GET', 'status' => 404]);

        $durationHistogram = $meter->createHistogram('http.request.duration', 'ms');
        $durationHistogram->record(45.5, ['method' => 'GET']);
        $durationHistogram->record(120.3, ['method' => 'POST']);

        $memoryGauge = $meter->createGauge('system.memory.usage', '%');
        $memoryGauge->record(75.5, ['host' => 'server-1']);

        $cpuGauge = $meter->createGauge('system.cpu.usage', '%');
        $cpuGauge->record(42.3, ['host' => 'server-1']);

        $queueCounter = $meter->createUpDownCounter('queue.size');
        $queueCounter->add(5, ['queue' => 'default']);
        $queueCounter->add(-3, ['queue' => 'default']);

        foreach ($meter->collect() as $metric) {
            $meter->processor()->process($metric);
        }
        $processor->flush();

        $metrics = $processor->metrics();
        $counterMetrics = array_filter($metrics, static fn($m) => $m->type === MetricType::COUNTER);
        $histogramMetrics = array_filter($metrics, static fn($m) => $m->type === MetricType::HISTOGRAM);
        $gaugeMetrics = array_filter($metrics, static fn($m) => $m->type === MetricType::GAUGE);
        $upDownCounterMetrics = array_filter($metrics, static fn($m) => $m->type === MetricType::UP_DOWN_COUNTER);

        static::assertCount(8, $metrics);
        static::assertCount(3, $counterMetrics);
        static::assertCount(2, $histogramMetrics);
        static::assertCount(2, $gaugeMetrics);
        static::assertCount(1, $upDownCounterMetrics);
    }

    public function test_filtering_metrics_by_name(): void
    {
        $processor = $this->createProcessor();
        $provider = new MeterProvider($processor, $this->clock);
        $meter = $provider->meter($this->resource, 'test', '1.0');

        $requestsCounter = $meter->createCounter('requests.total');
        $requestsCounter->add(10);
        $requestsCounter->add(5);

        $errorsCounter = $meter->createCounter('errors.total');
        $errorsCounter->add(2);

        $activeGauge = $meter->createGauge('requests.active');
        $activeGauge->record(3);

        foreach ($meter->collect() as $metric) {
            $meter->processor()->process($metric);
        }
        $processor->flush();

        $metrics = $processor->metrics();
        $requestsMetrics = array_filter($metrics, static fn($m) => $m->name === 'requests.total');
        $errorsMetrics = array_filter($metrics, static fn($m) => $m->name === 'errors.total');

        static::assertCount(3, $metrics);
        static::assertCount(1, $requestsMetrics);
        static::assertCount(1, $errorsMetrics);
    }

    public function test_metric_attributes_are_captured(): void
    {
        $processor = $this->createProcessor();
        $provider = new MeterProvider($processor, $this->clock);
        $meter = $provider->meter($this->resource, 'test', '1.0');

        $counter = $meter->createCounter('http.requests');
        $counter->add(1, [
            'method' => 'GET',
            'status' => 200,
            'path' => '/api/users',
        ]);

        foreach ($meter->collect() as $metric) {
            $meter->processor()->process($metric);
        }
        $processor->flush();

        $metric = $processor->metrics()[0];

        static::assertSame('http.requests', $metric->name);
        static::assertSame(MetricType::COUNTER, $metric->type);
        static::assertSame(1, $metric->value);
        static::assertSame(
            [
                'method' => 'GET',
                'status' => 200,
                'path' => '/api/users',
            ],
            $metric->attributes->normalize(),
        );
    }

    public function test_metric_unit_and_description_are_captured(): void
    {
        $processor = $this->createProcessor();
        $provider = new MeterProvider($processor, $this->clock);
        $meter = $provider->meter($this->resource, 'test', '1.0');

        $histogram = $meter->createHistogram('request.duration', 'ms', 'Request duration in milliseconds');
        $histogram->record(125.5, ['endpoint' => '/api/data']);

        foreach ($meter->collect() as $metric) {
            $meter->processor()->process($metric);
        }
        $processor->flush();

        $metric = $processor->metrics()[0];

        static::assertSame('ms', $metric->unit);
        static::assertSame('Request duration in milliseconds', $metric->description);
    }

    public function test_multiple_services_recording_metrics(): void
    {
        $processor = $this->createProcessor();
        $provider = new MeterProvider($processor, $this->clock);

        $httpMeter = $provider->meter($this->resource, 'http-server', '1.0.0');
        $dbMeter = $provider->meter($this->resource, 'database', '2.0.0');
        $cacheMeter = $provider->meter($this->resource, 'cache', '1.5.0');

        $httpMeter->createCounter('http.requests.received')->add(1, ['path' => '/api/users']);
        $cacheMeter->createCounter('cache.misses')->add(1, ['key' => 'users:list']);
        $dbMeter->createHistogram('db.query.duration', 'ms')->record(45, ['query' => 'SELECT']);
        $cacheMeter->createCounter('cache.writes')->add(1, ['key' => 'users:list']);
        $httpMeter->createHistogram('http.request.duration', 'ms')->record(52, ['path' => '/api/users']);

        foreach ([$httpMeter, $dbMeter, $cacheMeter] as $meter) {
            foreach ($meter->collect() as $metric) {
                $meter->processor()->process($metric);
            }
        }
        $processor->flush();

        $metrics = $processor->metrics();
        $counterMetrics = array_filter($metrics, static fn($m) => $m->type === MetricType::COUNTER);
        $histogramMetrics = array_filter($metrics, static fn($m) => $m->type === MetricType::HISTOGRAM);

        static::assertCount(5, $metrics);
        static::assertCount(3, $counterMetrics);
        static::assertCount(2, $histogramMetrics);
    }

    public function test_provider_creates_new_meter_each_time(): void
    {
        $processor = $this->createProcessor();
        $provider = new MeterProvider($processor, $this->clock);

        $meter1 = $provider->meter($this->resource, 'service-a', '1.0');
        $meter2 = $provider->meter($this->resource, 'service-a', '1.0');

        static::assertNotSame($meter1, $meter2);
        static::assertSame($meter1->name(), $meter2->name());
        static::assertSame($meter1->version(), $meter2->version());

        $meter1->createCounter('requests')->add(1);
        $meter2->createCounter('requests')->add(1);

        foreach ($meter1->collect() as $metric) {
            $meter1->processor()->process($metric);
        }

        foreach ($meter2->collect() as $metric) {
            $meter2->processor()->process($metric);
        }
        $processor->flush();

        static::assertCount(2, $processor->metrics());
    }

    public function test_timestamp_is_set_from_clock(): void
    {
        $processor = $this->createProcessor();
        $provider = new MeterProvider($processor, $this->clock);
        $meter = $provider->meter($this->resource, 'test', '1.0');

        $meter->createCounter('requests')->add(1);

        foreach ($meter->collect() as $metric) {
            $meter->processor()->process($metric);
        }
        $processor->flush();

        $metric = $processor->metrics()[0];

        static::assertSame('2024-01-01 12:00:00.123456', $metric->timestamp->format('Y-m-d H:i:s.u'));
    }

    private function createProcessor(): MemoryMetricProcessor
    {
        return new MemoryMetricProcessor(new VoidExporter());
    }
}
