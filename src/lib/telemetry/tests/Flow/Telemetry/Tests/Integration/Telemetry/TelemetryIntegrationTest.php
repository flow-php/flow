<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Integration\Telemetry;

use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\TracerProvider;
use PHPUnit\Framework\TestCase;
use Psr\Clock\ClockInterface;

final class TelemetryIntegrationTest extends TestCase
{
    private ClockInterface $clock;

    protected function setUp(): void
    {
        $this->clock = $this->createMock(ClockInterface::class);
        $this->clock->method('now')->willReturn(new \DateTimeImmutable('2024-01-01 12:00:00.123456'));
    }

    public function test_context_flows_through_all_signals(): void
    {
        $resource = ResourceMother::default();
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $contextStorage = new MemoryContextStorage();
        $tracerProvider = new TracerProvider($spanProcessor, $this->clock, $contextStorage);
        $meterProvider = new MeterProvider($metricProcessor, $this->clock);
        $loggerProvider = new LoggerProvider($logProcessor, $this->clock, $contextStorage);

        $telemetry = new Telemetry($resource, $tracerProvider, $meterProvider, $loggerProvider);

        $tracer = $telemetry->tracer('test-service');
        $logger = $telemetry->logger('test-service');

        $span = $tracer->span('process-data');

        $logger->info('Processing started', [
            'span.id' => $span->context()->spanId->toHex(),
            'trace.id' => $span->context()->traceId->toHex(),
        ]);

        $tracer->complete($span);

        $spans = $spanProcessor->endedSpans();
        $logs = $logProcessor->entries();

        static::assertCount(1, $spans);
        static::assertCount(1, $logs);

        $logEntry = $logs[0];
        static::assertTrue($logEntry->record->attributes->has('span.id'));
        static::assertTrue($logEntry->record->attributes->has('trace.id'));
        static::assertSame($span->context()->spanId->toHex(), $logEntry->record->attributes->get('span.id'));
        static::assertSame($span->context()->traceId->toHex(), $logEntry->record->attributes->get('trace.id'));
    }

    public function test_full_telemetry_workflow(): void
    {
        $resource = ResourceMother::default();
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $contextStorage = new MemoryContextStorage();
        $tracerProvider = new TracerProvider($spanProcessor, $this->clock, $contextStorage);
        $meterProvider = new MeterProvider($metricProcessor, $this->clock);
        $loggerProvider = new LoggerProvider($logProcessor, $this->clock, $contextStorage);

        $telemetry = new Telemetry($resource, $tracerProvider, $meterProvider, $loggerProvider);

        $tracer = $telemetry->tracer('test');
        $logger = $telemetry->logger('test');
        $meter = $telemetry->meter('test');

        $span = $tracer->span('process-data');
        $logger->info('Processing started', ['span.id' => $span->context()->spanId->toHex()]);
        $counter = $meter->createCounter('operations.total');
        $counter->add(1);

        $tracer->complete($span);
        $logger->info('Processing completed');

        foreach ($meter->collect() as $metric) {
            $meter->processor()->process($metric);
        }
        $telemetry->flush();

        static::assertCount(1, $spanProcessor->endedSpans());
        static::assertCount(2, $logProcessor->entries());
        static::assertCount(1, $metricProcessor->metrics());
    }

    public function test_multiple_services_recording_telemetry(): void
    {
        $resource = ResourceMother::default();
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $contextStorage = new MemoryContextStorage();
        $tracerProvider = new TracerProvider($spanProcessor, $this->clock, $contextStorage);
        $meterProvider = new MeterProvider($metricProcessor, $this->clock);
        $loggerProvider = new LoggerProvider($logProcessor, $this->clock, $contextStorage);

        $telemetry = new Telemetry($resource, $tracerProvider, $meterProvider, $loggerProvider);

        $httpTracer = $telemetry->tracer('http-server', '1.0.0');
        $dbTracer = $telemetry->tracer('database', '2.0.0');

        $httpLogger = $telemetry->logger('http-server', '1.0.0');
        $dbLogger = $telemetry->logger('database', '2.0.0');

        $httpMeter = $telemetry->meter('http-server', '1.0.0');
        $dbMeter = $telemetry->meter('database', '2.0.0');

        $httpSpan = $httpTracer->span('handle-request');
        $httpLogger->info('Request received', ['path' => '/api/users']);
        $httpCounter = $httpMeter->createCounter('http.requests.total');
        $httpCounter->add(1, ['method' => 'GET']);

        $dbSpan = $dbTracer->span('query-users');
        $dbLogger->debug('Executing query', ['table' => 'users']);
        $dbHistogram = $dbMeter->createHistogram('db.query.duration', 'ms');
        $dbHistogram->record(45.5, ['query' => 'SELECT']);
        $dbTracer->complete($dbSpan);

        $httpLogger->info('Request completed', ['status' => 200]);
        $httpHistogram = $httpMeter->createHistogram('http.request.duration', 'ms');
        $httpHistogram->record(52.3, ['path' => '/api/users']);
        $httpTracer->complete($httpSpan);

        foreach ([$httpMeter, $dbMeter] as $meter) {
            foreach ($meter->collect() as $metric) {
                $meter->processor()->process($metric);
            }
        }
        $telemetry->flush();

        static::assertCount(2, $spanProcessor->endedSpans());
        static::assertCount(3, $logProcessor->entries());
        static::assertCount(3, $metricProcessor->metrics());

        $counterMetrics = \array_filter($metricProcessor->metrics(), static fn($m) => $m->type === MetricType::COUNTER);
        $histogramMetrics = \array_filter(
            $metricProcessor->metrics(),
            static fn($m) => $m->type === MetricType::HISTOGRAM,
        );
        static::assertCount(1, $counterMetrics);
        static::assertCount(2, $histogramMetrics);
    }

    public function test_telemetry_flush_and_shutdown(): void
    {
        $resource = ResourceMother::default();
        $contextStorage = new MemoryContextStorage();
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $metricProcessor = new MemoryMetricProcessor(new VoidExporter());
        $logProcessor = new MemoryLogProcessor(new VoidExporter());
        $tracerProvider = new TracerProvider($spanProcessor, $this->clock, $contextStorage);
        $meterProvider = new MeterProvider($metricProcessor, $this->clock);
        $loggerProvider = new LoggerProvider($logProcessor, $this->clock, $contextStorage);

        $telemetry = new Telemetry($resource, $tracerProvider, $meterProvider, $loggerProvider);

        $telemetry->tracer('test')->span('test-span');
        $counter = $telemetry->meter('test')->createCounter('test-counter');
        $counter->add(1);
        $telemetry->logger('test')->info('test message');

        static::assertTrue($telemetry->flush());
        static::assertTrue($telemetry->shutdown());
    }
}
