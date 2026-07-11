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

final class CurlTransportIntegrationTest extends IntegrationTestCase
{
    public function test_send_exports_synchronously_without_shutdown(): void
    {
        if (!extension_loaded('curl')) {
            static::markTestSkipped('ext-curl is required');
        }

        $spansBefore = $this->otelContext->collectorMetrics()->getAcceptedSpans();

        $transport = new CurlTransport($this->otelContext->httpEndpoint(), otlp_json_serializer(), otlp_curl_options());

        $transport->send(Signals::traces($this->createSpans()));

        $spansAfter = $spansBefore;

        for ($i = 0; $i < 100; $i++) {
            $spansAfter = $this->otelContext->collectorMetrics()->getAcceptedSpans();

            if ($spansAfter > $spansBefore) {
                break;
            }

            usleep(10_000);
        }

        static::assertGreaterThan(
            $spansBefore,
            $spansAfter,
            'send() should export the /v1/traces batch synchronously, before any shutdown()',
        );

        $transport->shutdown();
    }

    /**
     * @return array<Span>
     */
    private function createSpans(): array
    {
        return [new Span(
            'sync-integration-span',
            SpanContext::create(TraceId::generate(), SpanId::generate()),
            SpanKind::INTERNAL,
            new DateTimeImmutable(),
            ResourceMother::default(),
            new InstrumentationScope('sync-integration', '1.0.0'),
        )];
    }
}
