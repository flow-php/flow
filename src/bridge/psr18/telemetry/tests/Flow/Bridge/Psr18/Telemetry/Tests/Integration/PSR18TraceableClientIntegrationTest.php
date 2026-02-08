<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr18\Telemetry\Tests\Integration;

use function Flow\Bridge\Psr18\Telemetry\DSL\psr18_traceable_client;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\{MemoryLogProcessor, MemoryMetricProcessor, MemorySpanProcessor};
use Flow\Telemetry\Provider\Void\{VoidLogExporter, VoidMetricExporter, VoidSpanExporter};
use Flow\Telemetry\{Resource, Telemetry};
use Flow\Telemetry\Tracer\{SpanKind, TracerProvider};
use Nyholm\Psr7\Request;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpClient\Psr18Client;

final class PSR18TraceableClientIntegrationTest extends TestCase
{
    public function test_real_http_request_creates_span() : void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidSpanExporter());
        $telemetry = $this->createTelemetry($spanProcessor);

        $httpClient = new Psr18Client();
        $traceableClient = psr18_traceable_client($httpClient, $telemetry);

        $request = new Request('GET', 'http://flow-php.com');
        $response = $traceableClient->sendRequest($request);

        self::assertGreaterThanOrEqual(200, $response->getStatusCode());
        self::assertLessThan(400, $response->getStatusCode());

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('GET flow-php.com', $span->name());
        self::assertSame(SpanKind::CLIENT, $span->kind());

        $attributes = $span->attributes();
        self::assertSame('GET', $attributes['http.method']);
        self::assertSame('http://flow-php.com', $attributes['http.url']);
        self::assertSame('http', $attributes['http.scheme']);
        self::assertSame('flow-php.com', $attributes['http.host']);
        self::assertArrayHasKey('http.status_code', $attributes);

        self::assertNotNull($span->status());
        self::assertTrue($span->status()->isOk());
    }

    private function createTelemetry(MemorySpanProcessor $spanProcessor) : Telemetry
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();

        return new Telemetry(
            Resource::create([
                'service.name' => 'integration-test',
                'service.version' => '1.0.0',
            ]),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider(new MemoryMetricProcessor(new VoidMetricExporter()), $clock),
            new LoggerProvider(new MemoryLogProcessor(new VoidLogExporter()), $clock, $contextStorage),
        );
    }
}
