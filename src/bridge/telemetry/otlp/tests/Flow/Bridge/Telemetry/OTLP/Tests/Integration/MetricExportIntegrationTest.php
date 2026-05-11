<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Integration;

use Flow\Bridge\Telemetry\OTLP\Tests\Context\TransportConfiguration;
use Flow\Telemetry\Resource;
use PHPUnit\Framework\Attributes\DataProvider;

final class MetricExportIntegrationTest extends IntegrationTestCase
{
    #[DataProvider('transportProvider')]
    public function test_exports_counter(TransportConfiguration $config): void
    {
        $metricsBefore = $this->otelContext->collectorMetrics()->getAcceptedMetricPoints();

        $telemetry = $this->otelContext->createTelemetry($config);
        $meter = $telemetry->meter('test-meter');

        $meter->createCounter('test.counter')->add(1);

        $telemetry->shutdown();

        static::assertGreaterThan(
            $metricsBefore,
            $this->otelContext->collectorMetrics()->waitForMetricPoints($metricsBefore),
            'Collector should have received counter metric',
        );
    }

    #[DataProvider('transportProvider')]
    public function test_exports_counter_with_attributes(TransportConfiguration $config): void
    {
        $metricsBefore = $this->otelContext->collectorMetrics()->getAcceptedMetricPoints();

        $telemetry = $this->otelContext->createTelemetry($config);
        $meter = $telemetry->meter('test-meter');

        $meter->createCounter('test.counter.attrs')->add(1, [
            'http.method' => 'GET',
            'http.status_code' => 200,
        ]);

        $telemetry->shutdown();

        static::assertGreaterThan(
            $metricsBefore,
            $this->otelContext->collectorMetrics()->waitForMetricPoints($metricsBefore),
            'Collector should have received counter with attributes',
        );
    }

    #[DataProvider('transportProvider')]
    public function test_exports_gauge(TransportConfiguration $config): void
    {
        $metricsBefore = $this->otelContext->collectorMetrics()->getAcceptedMetricPoints();

        $telemetry = $this->otelContext->createTelemetry($config);
        $meter = $telemetry->meter('test-meter');

        $meter->createGauge('test.gauge')->record(42.5);

        $telemetry->shutdown();

        static::assertGreaterThan(
            $metricsBefore,
            $this->otelContext->collectorMetrics()->waitForMetricPoints($metricsBefore),
            'Collector should have received gauge metric',
        );
    }

    #[DataProvider('transportProvider')]
    public function test_exports_histogram(TransportConfiguration $config): void
    {
        $metricsBefore = $this->otelContext->collectorMetrics()->getAcceptedMetricPoints();

        $telemetry = $this->otelContext->createTelemetry($config);
        $meter = $telemetry->meter('test-meter');

        $meter->createHistogram('test.histogram', 'ms')->record(150.0);

        $telemetry->shutdown();

        static::assertGreaterThan(
            $metricsBefore,
            $this->otelContext->collectorMetrics()->waitForMetricPoints($metricsBefore),
            'Collector should have received histogram metric',
        );
    }

    #[DataProvider('transportProvider')]
    public function test_exports_metric_with_description(TransportConfiguration $config): void
    {
        $metricsBefore = $this->otelContext->collectorMetrics()->getAcceptedMetricPoints();

        $telemetry = $this->otelContext->createTelemetry($config);
        $meter = $telemetry->meter('test-meter');

        $meter->createCounter('test.described.metric', 'requests', 'Total number of HTTP requests')->add(1);

        $telemetry->shutdown();

        static::assertGreaterThan(
            $metricsBefore,
            $this->otelContext->collectorMetrics()->waitForMetricPoints($metricsBefore),
            'Collector should have received metric with description',
        );
    }

    #[DataProvider('transportProvider')]
    public function test_exports_metric_with_resource_attributes(TransportConfiguration $config): void
    {
        $metricsBefore = $this->otelContext->collectorMetrics()->getAcceptedMetricPoints();

        $telemetry = $this->otelContext->createTelemetry($config, Resource::create([
            'service.name' => 'flow-php-otlp-bridge-tests',
            'service.version' => '2.0.0',
        ]));
        $meter = $telemetry->meter('test-meter');

        $meter->createCounter('test.resource.metric')->add(1);

        $telemetry->shutdown();

        static::assertGreaterThan(
            $metricsBefore,
            $this->otelContext->collectorMetrics()->waitForMetricPoints($metricsBefore),
            'Collector should have received metric with resource',
        );
    }

    #[DataProvider('transportProvider')]
    public function test_exports_multiple_metrics(TransportConfiguration $config): void
    {
        $metricsBefore = $this->otelContext->collectorMetrics()->getAcceptedMetricPoints();

        $telemetry = $this->otelContext->createTelemetry($config);
        $meter = $telemetry->meter('test-meter');

        $meter->createCounter('test.multiple.counter')->add(1);
        $meter->createGauge('test.multiple.gauge')->record(50);
        $meter->createHistogram('test.multiple.histogram')->record(100.0);

        $telemetry->shutdown();

        static::assertGreaterThanOrEqual(
            $metricsBefore + 3,
            $this->otelContext->collectorMetrics()->waitForMetricPoints($metricsBefore + 2),
            'Collector should have received 3 metrics',
        );
    }

    #[DataProvider('transportProvider')]
    public function test_exports_up_down_counter(TransportConfiguration $config): void
    {
        $metricsBefore = $this->otelContext->collectorMetrics()->getAcceptedMetricPoints();

        $telemetry = $this->otelContext->createTelemetry($config);
        $meter = $telemetry->meter('test-meter');

        $upDownCounter = $meter->createUpDownCounter('test.updown');
        $upDownCounter->add(5);
        $upDownCounter->add(-3);

        $telemetry->shutdown();

        static::assertGreaterThan(
            $metricsBefore,
            $this->otelContext->collectorMetrics()->waitForMetricPoints($metricsBefore),
            'Collector should have received up-down counter metric',
        );
    }
}
