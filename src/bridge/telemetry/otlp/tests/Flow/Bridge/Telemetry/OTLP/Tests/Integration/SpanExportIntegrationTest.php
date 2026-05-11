<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Integration;

use Flow\Bridge\Telemetry\OTLP\Tests\Context\TransportConfiguration;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tracer\GenericEvent;
use Flow\Telemetry\Tracer\SpanContext;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanLink;
use Flow\Telemetry\Tracer\SpanStatus;
use PHPUnit\Framework\Attributes\DataProvider;

final class SpanExportIntegrationTest extends IntegrationTestCase
{
    #[DataProvider('transportProvider')]
    public function test_exports_nested_spans_with_parent_relationship(TransportConfiguration $config): void
    {
        $spansBefore = $this->otelContext->collectorMetrics()->getAcceptedSpans();

        $telemetry = $this->otelContext->createTelemetry($config);
        $tracer = $telemetry->tracer('test-component');

        $parentSpan = $tracer->span('parent-span');
        $childSpan = $tracer->span('child-span');
        $tracer->complete($childSpan);
        $tracer->complete($parentSpan);

        $telemetry->shutdown();

        static::assertGreaterThanOrEqual(
            $spansBefore + 2,
            $this->otelContext->collectorMetrics()->waitForSpans($spansBefore + 1),
            'Collector should have received 2 spans',
        );
    }

    #[DataProvider('transportProvider')]
    public function test_exports_single_span(TransportConfiguration $config): void
    {
        $spansBefore = $this->otelContext->collectorMetrics()->getAcceptedSpans();

        $telemetry = $this->otelContext->createTelemetry($config);
        $tracer = $telemetry->tracer('test-component');

        $span = $tracer->span('test-span');
        $tracer->complete($span);

        $telemetry->shutdown();

        static::assertGreaterThan(
            $spansBefore,
            $this->otelContext->collectorMetrics()->waitForSpans($spansBefore),
            'Collector should have received the span',
        );
    }

    #[DataProvider('transportProvider')]
    public function test_exports_span_with_attributes(TransportConfiguration $config): void
    {
        $spansBefore = $this->otelContext->collectorMetrics()->getAcceptedSpans();

        $telemetry = $this->otelContext->createTelemetry($config);
        $tracer = $telemetry->tracer('test-component');

        $span = $tracer->span('attribute-test');
        $span->setAttribute('http.method', 'GET');
        $span->setAttribute('http.status_code', 200);
        $span->setAttribute('request.duration', 1.5);
        $span->setAttribute('cache.hit', true);
        $span->setAttribute('tags', ['a', 'b', 'c']);
        $tracer->complete($span);

        $telemetry->shutdown();

        static::assertGreaterThan(
            $spansBefore,
            $this->otelContext->collectorMetrics()->waitForSpans($spansBefore),
            'Collector should have received span with attributes',
        );
    }

    #[DataProvider('transportProvider')]
    public function test_exports_span_with_events(TransportConfiguration $config): void
    {
        $spansBefore = $this->otelContext->collectorMetrics()->getAcceptedSpans();

        $telemetry = $this->otelContext->createTelemetry($config);
        $tracer = $telemetry->tracer('test-component');

        $span = $tracer->span('event-test');
        $span->recordEvent(GenericEvent::create('cache.hit', new \DateTimeImmutable(), ['cache.key' => 'user:123']));
        $span->recordEvent(GenericEvent::create('db.query', new \DateTimeImmutable(), [
            'db.statement' => 'SELECT * FROM users',
        ]));
        $tracer->complete($span);

        $telemetry->shutdown();

        static::assertGreaterThan(
            $spansBefore,
            $this->otelContext->collectorMetrics()->waitForSpans($spansBefore),
            'Collector should have received span with events',
        );
    }

    #[DataProvider('transportProvider')]
    public function test_exports_span_with_kind(TransportConfiguration $config): void
    {
        $spansBefore = $this->otelContext->collectorMetrics()->getAcceptedSpans();

        $telemetry = $this->otelContext->createTelemetry($config);
        $tracer = $telemetry->tracer('test-component');

        $span = $tracer->span('kind-test', SpanKind::SERVER);
        $tracer->complete($span);

        $telemetry->shutdown();

        static::assertGreaterThan(
            $spansBefore,
            $this->otelContext->collectorMetrics()->waitForSpans($spansBefore),
            'Collector should have received span with kind',
        );
    }

    #[DataProvider('transportProvider')]
    public function test_exports_span_with_links(TransportConfiguration $config): void
    {
        $spansBefore = $this->otelContext->collectorMetrics()->getAcceptedSpans();

        $telemetry = $this->otelContext->createTelemetry($config);
        $tracer = $telemetry->tracer('test-component');

        $linkedContext = SpanContext::create(TraceId::generate(), SpanId::generate());

        $span = $tracer->span(
            'link-test',
            SpanKind::INTERNAL,
            [],
            [
                SpanLink::create($linkedContext, ['link.reason' => 'batch-trigger']),
            ],
        );
        $tracer->complete($span);

        $telemetry->shutdown();

        static::assertGreaterThan(
            $spansBefore,
            $this->otelContext->collectorMetrics()->waitForSpans($spansBefore),
            'Collector should have received span with links',
        );
    }

    #[DataProvider('transportProvider')]
    public function test_exports_span_with_resource_attributes(TransportConfiguration $config): void
    {
        $spansBefore = $this->otelContext->collectorMetrics()->getAcceptedSpans();

        $telemetry = $this->otelContext->createTelemetry($config, Resource::create([
            'service.name' => 'flow-php-otlp-bridge-tests',
            'service.version' => '1.2.3',
            'deployment.environment' => 'testing',
        ]));
        $tracer = $telemetry->tracer('test-component');

        $span = $tracer->span('resource-test');
        $tracer->complete($span);

        $telemetry->shutdown();

        static::assertGreaterThan(
            $spansBefore,
            $this->otelContext->collectorMetrics()->waitForSpans($spansBefore),
            'Collector should have received span with resource',
        );
    }

    #[DataProvider('transportProvider')]
    public function test_exports_span_with_status(TransportConfiguration $config): void
    {
        $spansBefore = $this->otelContext->collectorMetrics()->getAcceptedSpans();

        $telemetry = $this->otelContext->createTelemetry($config);
        $tracer = $telemetry->tracer('test-component');

        $span = $tracer->span('status-test');
        $span->setStatus(SpanStatus::error('Something went wrong'));
        $tracer->complete($span);

        $telemetry->shutdown();

        static::assertGreaterThan(
            $spansBefore,
            $this->otelContext->collectorMetrics()->waitForSpans($spansBefore),
            'Collector should have received span with status',
        );
    }
}
