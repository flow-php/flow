<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr18\Telemetry\Tests\Unit;

use function Flow\Bridge\Psr18\Telemetry\DSL\psr18_traceable_client;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\{MemoryLogProcessor, MemoryMetricProcessor, MemorySpanProcessor};
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\{Resource, Telemetry};
use Flow\Telemetry\Tracer\{SpanKind, TracerProvider};
use Nyholm\Psr7\{Request, Response};
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientInterface;

final class PSR18TraceableClientTest extends TestCase
{
    public function test_exception_is_recorded_and_rethrown() : void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $exception = new \RuntimeException('Connection failed');
        $mockClient = $this->createMock(ClientInterface::class);
        $mockClient->method('sendRequest')->willThrowException($exception);

        $traceableClient = psr18_traceable_client($mockClient, $telemetry);

        $request = new Request('GET', 'https://api.example.com/users');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Connection failed');

        try {
            $traceableClient->sendRequest($request);
        } finally {
            $spans = $spanProcessor->endedSpans();
            self::assertCount(1, $spans);
            $span = $spans[0];

            $events = $span->events();
            self::assertCount(1, $events);
            self::assertSame('exception', $events[0]->name());

            $eventAttributes = $events[0]->attributes();
            self::assertSame(\RuntimeException::class, $eventAttributes['exception.type']);
            self::assertSame('Connection failed', $eventAttributes['exception.message']);

            self::assertNotNull($span->status());
            self::assertTrue($span->status()->isError());
            self::assertSame('Connection failed', $span->status()->description);
        }
    }

    public function test_request_with_4xx_status_creates_error_span() : void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $mockClient = $this->createMock(ClientInterface::class);
        $mockClient->method('sendRequest')->willReturn(new Response(404));

        $traceableClient = psr18_traceable_client($mockClient, $telemetry);

        $request = new Request('GET', 'https://api.example.com/users/123');
        $traceableClient->sendRequest($request);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        $span = $spans[0];

        self::assertNotNull($span->status());
        self::assertTrue($span->status()->isError());
        self::assertSame('HTTP 404', $span->status()->description);
        self::assertSame(404, $span->attributes()['http.response.status_code']);
    }

    public function test_request_with_5xx_status_creates_error_span() : void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $mockClient = $this->createMock(ClientInterface::class);
        $mockClient->method('sendRequest')->willReturn(new Response(500));

        $traceableClient = psr18_traceable_client($mockClient, $telemetry);

        $request = new Request('GET', 'https://api.example.com/users');
        $traceableClient->sendRequest($request);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        $span = $spans[0];

        self::assertNotNull($span->status());
        self::assertTrue($span->status()->isError());
        self::assertSame('HTTP 500', $span->status()->description);
        self::assertSame(500, $span->attributes()['http.response.status_code']);
    }

    public function test_span_has_correct_attributes() : void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $mockClient = $this->createMock(ClientInterface::class);
        $mockClient->method('sendRequest')->willReturn(new Response(200));

        $traceableClient = psr18_traceable_client($mockClient, $telemetry);

        $request = new Request('POST', 'https://api.example.com:8080/users?page=1');
        $traceableClient->sendRequest($request);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        $span = $spans[0];

        $attributes = $span->attributes();
        self::assertSame('POST', $attributes['http.request.method']);
        self::assertSame('https://api.example.com:8080/users?page=1', $attributes['url.full']);
        self::assertSame('https', $attributes['url.scheme']);
        self::assertSame('api.example.com', $attributes['server.address']);
        self::assertSame(8080, $attributes['server.port']);
        self::assertSame(200, $attributes['http.response.status_code']);
    }

    public function test_span_kind_is_client() : void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $mockClient = $this->createMock(ClientInterface::class);
        $mockClient->method('sendRequest')->willReturn(new Response(200));

        $traceableClient = psr18_traceable_client($mockClient, $telemetry);

        $request = new Request('GET', 'https://api.example.com/users');
        $traceableClient->sendRequest($request);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame(SpanKind::CLIENT, $spans[0]->kind());
    }

    public function test_successful_request_creates_span_with_ok_status() : void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $mockClient = $this->createMock(ClientInterface::class);
        $mockClient->method('sendRequest')->willReturn(new Response(200));

        $traceableClient = psr18_traceable_client($mockClient, $telemetry);

        $request = new Request('GET', 'https://api.example.com/users');
        $response = $traceableClient->sendRequest($request);

        self::assertSame(200, $response->getStatusCode());

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        $span = $spans[0];

        self::assertSame('GET api.example.com', $span->name());
        self::assertNotNull($span->status());
        self::assertTrue($span->status()->isOk());
    }

    private function createTelemetry(MemorySpanProcessor $spanProcessor) : Telemetry
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
