<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr18\Telemetry\Tests\Unit;

use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Sampler\AlwaysOnSampler;
use Flow\Telemetry\Tracer\Sampler\SuppressingSampler;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\TracerProvider;
use Nyholm\Psr7\Request;
use Nyholm\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientInterface;
use RuntimeException;

use function Flow\Bridge\Psr18\Telemetry\DSL\psr18_traceable_client;

final class PSR18TraceableClientTest extends TestCase
{
    public function test_emits_no_spans_when_tracing_is_suppressed(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage(Context::root()->withSuppressedTracing());
        $telemetry = new Telemetry(
            Resource::create(['service.name' => 'test-service']),
            new TracerProvider($spanProcessor, $clock, $contextStorage, new SuppressingSampler(new AlwaysOnSampler())),
            new MeterProvider(new MemoryMetricProcessor(new VoidExporter()), $clock),
            new LoggerProvider(new MemoryLogProcessor(new VoidExporter()), $clock, $contextStorage),
        );

        $client = $this->createStub(ClientInterface::class);
        $client->method('sendRequest')->willReturn(new Response(200));

        psr18_traceable_client($client, $telemetry)->sendRequest(new Request('GET', 'https://api.example.com/users'));

        static::assertCount(
            0,
            $spanProcessor->endedSpans(),
            'PSR-18 client instrumentation must emit no spans while tracing is suppressed',
        );
    }

    public function test_exception_is_recorded_and_rethrown(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $exception = new RuntimeException('Connection failed');
        $mockClient = $this->createStub(ClientInterface::class);
        $mockClient->method('sendRequest')->willThrowException($exception);

        $traceableClient = psr18_traceable_client($mockClient, $telemetry);

        $request = new Request('GET', 'https://api.example.com/users');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Connection failed');

        try {
            $traceableClient->sendRequest($request);
        } finally {
            $spans = $spanProcessor->endedSpans();
            static::assertCount(1, $spans);
            $span = $spans[0];

            $events = $span->events();
            static::assertCount(1, $events);
            static::assertSame('exception', $events[0]->name());

            $eventAttributes = $events[0]->attributes();
            static::assertSame(RuntimeException::class, $eventAttributes['exception.type']);
            static::assertSame('Connection failed', $eventAttributes['exception.message']);

            $status = $span->status();

            if ($status === null) {
                static::fail('Expected span to have status');
            }

            static::assertTrue($status->isError());
            static::assertSame('Connection failed', $status->description);
            static::assertSame(RuntimeException::class, $span->attributes()['error.type']);
        }
    }

    public function test_request_with_4xx_status_creates_error_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $mockClient = $this->createStub(ClientInterface::class);
        $mockClient->method('sendRequest')->willReturn(new Response(404));

        $traceableClient = psr18_traceable_client($mockClient, $telemetry);

        $request = new Request('GET', 'https://api.example.com/users/123');
        $traceableClient->sendRequest($request);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        $span = $spans[0];

        $status = $span->status();

        if ($status === null) {
            static::fail('Expected span to have status');
        }

        static::assertTrue($status->isError());
        static::assertSame('HTTP 404', $status->description);
        static::assertSame(404, $span->attributes()['http.response.status_code']);
        static::assertSame('404', $span->attributes()['error.type']);
    }

    public function test_request_with_5xx_status_creates_error_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $mockClient = $this->createStub(ClientInterface::class);
        $mockClient->method('sendRequest')->willReturn(new Response(500));

        $traceableClient = psr18_traceable_client($mockClient, $telemetry);

        $request = new Request('GET', 'https://api.example.com/users');
        $traceableClient->sendRequest($request);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        $span = $spans[0];

        $status = $span->status();

        if ($status === null) {
            static::fail('Expected span to have status');
        }

        static::assertTrue($status->isError());
        static::assertSame('HTTP 500', $status->description);
        static::assertSame(500, $span->attributes()['http.response.status_code']);
        static::assertSame('500', $span->attributes()['error.type']);
    }

    public function test_span_has_correct_attributes(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $mockClient = $this->createStub(ClientInterface::class);
        $mockClient->method('sendRequest')->willReturn(new Response(200));

        $traceableClient = psr18_traceable_client($mockClient, $telemetry);

        $request = new Request('POST', 'https://api.example.com:8080/users?page=1');
        $traceableClient->sendRequest($request);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        $span = $spans[0];

        $attributes = $span->attributes();
        static::assertSame('POST', $attributes['http.request.method']);
        static::assertSame('https://api.example.com:8080/users?page=1', $attributes['url.full']);
        static::assertSame('https', $attributes['url.scheme']);
        static::assertSame('api.example.com', $attributes['server.address']);
        static::assertSame(8080, $attributes['server.port']);
        static::assertSame(200, $attributes['http.response.status_code']);
    }

    public function test_span_kind_is_client(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $mockClient = $this->createStub(ClientInterface::class);
        $mockClient->method('sendRequest')->willReturn(new Response(200));

        $traceableClient = psr18_traceable_client($mockClient, $telemetry);

        $request = new Request('GET', 'https://api.example.com/users');
        $traceableClient->sendRequest($request);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame(SpanKind::CLIENT, $spans[0]->kind());
    }

    public function test_successful_request_creates_span_with_ok_status(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $mockClient = $this->createStub(ClientInterface::class);
        $mockClient->method('sendRequest')->willReturn(new Response(200));

        $traceableClient = psr18_traceable_client($mockClient, $telemetry);

        $request = new Request('GET', 'https://api.example.com/users');
        $response = $traceableClient->sendRequest($request);

        static::assertSame(200, $response->getStatusCode());

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        $span = $spans[0];

        static::assertSame('GET', $span->name());
        // OTEL HTTP semconv: server.port is Required on client spans, even for scheme defaults.
        static::assertSame(443, $span->attributes()['server.port']);

        // OTEL spec: instrumentation leaves the status Unset on success.
        static::assertNull($span->status());
        static::assertArrayNotHasKey('error.type', $span->attributes());
    }

    private function createTelemetry(MemorySpanProcessor $spanProcessor): Telemetry
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();

        return new Telemetry(
            Resource::create([
                'service.name' => 'test-service',
                'service.version' => '1.0.0',
            ]),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider(new MemoryMetricProcessor(new VoidExporter()), $clock),
            new LoggerProvider(new MemoryLogProcessor(new VoidExporter()), $clock, $contextStorage),
        );
    }
}
