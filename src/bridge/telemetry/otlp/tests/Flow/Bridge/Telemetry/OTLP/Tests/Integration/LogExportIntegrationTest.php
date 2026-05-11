<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Integration;

use Flow\Bridge\Telemetry\OTLP\Tests\Context\TransportConfiguration;
use Flow\Telemetry\Resource;
use PHPUnit\Framework\Attributes\DataProvider;

final class LogExportIntegrationTest extends IntegrationTestCase
{
    #[DataProvider('transportProvider')]
    public function test_exports_log_with_attributes(TransportConfiguration $config): void
    {
        $logsBefore = $this->otelContext->collectorMetrics()->getAcceptedLogRecords();

        $telemetry = $this->otelContext->createTelemetry($config);
        $logger = $telemetry->logger('test-logger');

        $logger->info('Log with attributes', [
            'request.id' => 'abc-123',
            'user.id' => 12345,
            'duration.ms' => 42.5,
            'success' => true,
        ]);

        $telemetry->shutdown();

        static::assertGreaterThan(
            $logsBefore,
            $this->otelContext->collectorMetrics()->waitForLogRecords($logsBefore),
            'Collector should have received log with attributes',
        );
    }

    #[DataProvider('transportProvider')]
    public function test_exports_log_with_resource_attributes(TransportConfiguration $config): void
    {
        $logsBefore = $this->otelContext->collectorMetrics()->getAcceptedLogRecords();

        $telemetry = $this->otelContext->createTelemetry($config, Resource::create([
            'service.name' => 'flow-php-otlp-bridge-tests',
            'service.version' => '3.0.0',
            'deployment.environment' => 'integration-test',
        ]));
        $logger = $telemetry->logger('test-logger');

        $logger->info('Resource log message');

        $telemetry->shutdown();

        static::assertGreaterThan(
            $logsBefore,
            $this->otelContext->collectorMetrics()->waitForLogRecords($logsBefore),
            'Collector should have received log with resource',
        );
    }

    #[DataProvider('transportProvider')]
    public function test_exports_log_with_severity(TransportConfiguration $config): void
    {
        $logsBefore = $this->otelContext->collectorMetrics()->getAcceptedLogRecords();

        $telemetry = $this->otelContext->createTelemetry($config);
        $logger = $telemetry->logger('test-logger');

        $logger->error('Error log message');

        $telemetry->shutdown();

        static::assertGreaterThan(
            $logsBefore,
            $this->otelContext->collectorMetrics()->waitForLogRecords($logsBefore),
            'Collector should have received log with severity',
        );
    }

    #[DataProvider('transportProvider')]
    public function test_exports_log_with_trace_context(TransportConfiguration $config): void
    {
        $logsBefore = $this->otelContext->collectorMetrics()->getAcceptedLogRecords();
        $spansBefore = $this->otelContext->collectorMetrics()->getAcceptedSpans();

        $telemetry = $this->otelContext->createTelemetry($config);
        $tracer = $telemetry->tracer('test-tracer');
        $logger = $telemetry->logger('test-logger');

        $span = $tracer->span('parent-operation');
        $logger->info('Log within span');
        $tracer->complete($span);

        $telemetry->shutdown();

        static::assertGreaterThan(
            $logsBefore,
            $this->otelContext->collectorMetrics()->waitForLogRecords($logsBefore),
            'Collector should have received log with trace context',
        );
        static::assertGreaterThan(
            $spansBefore,
            $this->otelContext->collectorMetrics()->waitForSpans($spansBefore),
            'Collector should have received the span',
        );
    }

    #[DataProvider('transportProvider')]
    public function test_exports_multiple_logs_in_sequence(TransportConfiguration $config): void
    {
        $logsBefore = $this->otelContext->collectorMetrics()->getAcceptedLogRecords();

        $telemetry = $this->otelContext->createTelemetry($config);
        $logger = $telemetry->logger('test-logger');

        $logger->info('First log message');
        $logger->debug('Second log message');
        $logger->warn('Third log message');

        $telemetry->shutdown();

        static::assertGreaterThanOrEqual(
            $logsBefore + 3,
            $this->otelContext->collectorMetrics()->waitForLogRecords($logsBefore + 2),
            'Collector should have received 3 logs',
        );
    }

    #[DataProvider('transportProvider')]
    public function test_exports_single_log(TransportConfiguration $config): void
    {
        $logsBefore = $this->otelContext->collectorMetrics()->getAcceptedLogRecords();

        $telemetry = $this->otelContext->createTelemetry($config);
        $logger = $telemetry->logger('test-logger');

        $logger->info('Test log message');

        $telemetry->shutdown();

        static::assertGreaterThan(
            $logsBefore,
            $this->otelContext->collectorMetrics()->waitForLogRecords($logsBefore),
            'Collector should have received the log',
        );
    }
}
