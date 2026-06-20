<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Integration;

use DateTimeImmutable;
use Flow\Bridge\Telemetry\OTLP\Transport\CurlTransport;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanContext;
use Flow\Telemetry\Tracer\SpanKind;

use function extension_loaded;
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_curl_options;
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_json_serializer;
use function usleep;

final class CurlTransportTickIntegrationTest extends IntegrationTestCase
{
    public function test_tick_drives_in_flight_request_to_completion_without_shutdown(): void
    {
        if (!extension_loaded('curl')) {
            static::markTestSkipped('ext-curl is required');
        }

        $spansBefore = $this->otelContext->collectorMetrics()->getAcceptedSpans();

        $transport = new CurlTransport(
            $this->otelContext->httpEndpoint(),
            otlp_json_serializer(),
            otlp_curl_options()->withTimeout(5000)->withConnectTimeout(1000),
        );

        $transport->send(Signals::traces($this->createSpans()));

        $spansAfter = $spansBefore;

        for ($i = 0; $i < 500; $i++) {
            $transport->tick();
            $spansAfter = $this->otelContext->collectorMetrics()->getAcceptedSpans();

            if ($spansAfter > $spansBefore) {
                break;
            }

            usleep(10_000);
        }

        static::assertGreaterThan(
            $spansBefore,
            $spansAfter,
            'tick() should have driven the in-flight /v1/traces request to completion without shutdown()',
        );

        $transport->shutdown();
    }

    /**
     * @return array<Span>
     */
    private function createSpans(): array
    {
        return [new Span(
            'tick-integration-span',
            SpanContext::create(TraceId::generate(), SpanId::generate()),
            SpanKind::INTERNAL,
            new DateTimeImmutable(),
            ResourceMother::default(),
            new InstrumentationScope('tick-integration', '1.0.0'),
        )];
    }
}
