<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr18\Telemetry\Tests\Integration;

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
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\TracerProvider;
use Nyholm\Psr7\Request;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Psr18Client;
use Symfony\Component\HttpClient\Response\MockResponse;

use function Flow\Bridge\Psr18\Telemetry\DSL\psr18_traceable_client;

final class PSR18TraceableClientIntegrationTest extends TestCase
{
    public function test_real_http_request_creates_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $httpClient = new Psr18Client(new MockHttpClient(new MockResponse('ok', ['http_code' => 200])));
        $traceableClient = psr18_traceable_client($httpClient, $telemetry);

        $url = 'http://127.0.0.1:8080/';
        $response = $traceableClient->sendRequest(new Request('GET', $url));

        static::assertSame(200, $response->getStatusCode());

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $span = $spans[0];
        static::assertSame('GET 127.0.0.1', $span->name());
        static::assertSame(SpanKind::CLIENT, $span->kind());

        $attributes = $span->attributes();
        static::assertSame('GET', $attributes['http.request.method']);
        static::assertSame($url, $attributes['url.full']);
        static::assertSame('http', $attributes['url.scheme']);
        static::assertSame('127.0.0.1', $attributes['server.address']);
        static::assertSame(8080, $attributes['server.port']);
        static::assertSame(200, $attributes['http.response.status_code']);

        // OTEL spec: instrumentation leaves the status Unset on success.
        static::assertNull($span->status());
    }

    private function createTelemetry(MemorySpanProcessor $spanProcessor): Telemetry
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();

        return new Telemetry(
            Resource::create([
                'service.name' => 'integration-test',
                'service.version' => '1.0.0',
            ]),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider(new MemoryMetricProcessor(new VoidExporter()), $clock),
            new LoggerProvider(new MemoryLogProcessor(new VoidExporter()), $clock, $contextStorage),
        );
    }
}
