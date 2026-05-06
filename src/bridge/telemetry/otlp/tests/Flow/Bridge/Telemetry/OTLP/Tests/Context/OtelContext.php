<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Context;

use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_exporter;
use function Flow\Telemetry\DSL\{batching_log_processor, batching_metric_processor, batching_span_processor, logger_provider, meter_provider, resource, telemetry, tracer_provider};
use Flow\Bridge\Telemetry\OTLP\Tests\Integration\CollectorMetrics;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\{Resource, Telemetry};
use Nyholm\Psr7\Factory\Psr17Factory;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\{RequestFactoryInterface, StreamFactoryInterface};
use Symfony\Component\HttpClient\Psr18Client;

/**
 * Test context for OTLP integration tests.
 *
 * Centralizes all test dependencies like HTTP clients, factories, and collector metrics.
 * This context is initialized once and provides access to all shared test infrastructure.
 */
final class OtelContext
{
    private static ?self $instance = null;

    private readonly CollectorMetrics $collectorMetrics;

    private readonly string $grpcEndpoint;

    private readonly ClientInterface $httpClient;

    private readonly string $httpEndpoint;

    private readonly Psr17Factory $psr17Factory;

    private function __construct()
    {
        $this->httpClient = new Psr18Client();
        $this->psr17Factory = new Psr17Factory();

        $httpEndpoint = \getenv('OTEL_RECEIVER_HTTP_ENDPOINT');
        $grpcEndpoint = \getenv('OTEL_RECEIVER_GRPC_ENDPOINT');
        $metricsEndpoint = \getenv('OTEL_COLLECTOR_METRICS_ENDPOINT');

        if ($httpEndpoint === false) {
            throw new \RuntimeException('Missing required environment variable: OTEL_RECEIVER_HTTP_ENDPOINT');
        }

        if ($grpcEndpoint === false) {
            throw new \RuntimeException('Missing required environment variable: OTEL_RECEIVER_GRPC_ENDPOINT');
        }

        if ($metricsEndpoint === false) {
            throw new \RuntimeException('Missing required environment variable: OTEL_COLLECTOR_METRICS_ENDPOINT');
        }

        $this->httpEndpoint = $httpEndpoint;
        $this->grpcEndpoint = $grpcEndpoint;

        $this->collectorMetrics = new CollectorMetrics(
            $this->httpClient,
            $this->psr17Factory,
            $metricsEndpoint,
        );
    }

    public static function instance() : self
    {
        if (self::$instance === null) {
            self::$instance = new self();
        }

        return self::$instance;
    }

    /**
     * Get all available transport configurations for the current environment.
     *
     * @return list<TransportConfiguration>
     */
    public function availableTransports() : array
    {
        return TransportConfiguration::available();
    }

    public function collectorMetrics() : CollectorMetrics
    {
        return $this->collectorMetrics;
    }

    /**
     * Create a Telemetry instance configured for the given transport configuration.
     *
     * Creates separate transport instances for each exporter to avoid shutdown race conditions.
     */
    public function createTelemetry(TransportConfiguration $config, ?Resource $resource = null) : Telemetry
    {
        $resource ??= resource([
            'service.name' => 'flow-php-otlp-bridge-tests',
            'service.namespace' => 'flow-php',
            'deployment.environment' => 'test',
        ]);
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();
        $batchSize = 1;

        $spanProcessor = batching_span_processor(otlp_exporter($config->createTransport($this)), $batchSize);
        $metricProcessor = batching_metric_processor(otlp_exporter($config->createTransport($this)), $batchSize);
        $logProcessor = batching_log_processor(otlp_exporter($config->createTransport($this)), $batchSize);

        $tracerProvider = tracer_provider($spanProcessor, $clock, $contextStorage);
        $meterProvider = meter_provider($metricProcessor, $clock);
        $loggerProvider = logger_provider($logProcessor, $clock, $contextStorage);

        return telemetry($resource, $tracerProvider, $meterProvider, $loggerProvider);
    }

    public function grpcEndpoint() : string
    {
        return $this->grpcEndpoint;
    }

    public function httpClient() : ClientInterface
    {
        return $this->httpClient;
    }

    public function httpEndpoint() : string
    {
        return $this->httpEndpoint;
    }

    public function requestFactory() : RequestFactoryInterface
    {
        return $this->psr17Factory;
    }

    public function streamFactory() : StreamFactoryInterface
    {
        return $this->psr17Factory;
    }
}
