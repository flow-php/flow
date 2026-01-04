<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Transport;

use Flow\Bridge\Telemetry\OTLP\Serializer\GrpcSerializer;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Transport\{Transport, TransportException};
use Grpc\ChannelCredentials;
use Opentelemetry\Proto\Collector\Logs\V1\LogsServiceClient;
use Opentelemetry\Proto\Collector\Metrics\V1\MetricsServiceClient;
use Opentelemetry\Proto\Collector\Trace\V1\TraceServiceClient;

/**
 * gRPC transport for OTLP using the grpc PHP extension.
 *
 * Sends telemetry data as protobuf over gRPC to OTLP-compatible endpoints.
 * Requires the grpc PHP extension and google/protobuf + open-telemetry/gen-otlp-protobuf packages.
 *
 * Example usage:
 * ```php
 * $transport = new GrpcTransport(
 *     endpoint: 'localhost:4317',
 *     serializer: new ProtobufSerializer(),
 * );
 *
 * $transport->sendSpans($spans);
 * ```
 */
final class GrpcTransport implements Transport
{
    private ?LogsServiceClient $logsClient = null;

    private ?MetricsServiceClient $metricsClient = null;

    private ?TraceServiceClient $tracesClient = null;

    /**
     * @param string $endpoint gRPC endpoint (e.g., 'localhost:4317')
     * @param GrpcSerializer $serializer gRPC serializer for creating request messages
     * @param array<string, string> $headers Additional headers (metadata) to include in requests
     * @param bool $insecure Whether to use insecure channel credentials (default true for local dev)
     */
    public function __construct(
        private readonly string $endpoint,
        private readonly GrpcSerializer $serializer,
        private readonly array $headers = [],
        private readonly bool $insecure = true,
    ) {
        if (!\extension_loaded('grpc')) {
            throw new \RuntimeException(
                'The grpc PHP extension is required for GrpcTransport. '
                . 'Install it via: pecl install grpc'
            );
        }
    }

    /**
     * @param array<LogEntry> $entries
     */
    public function sendLogs(array $entries) : void
    {
        $request = $this->serializer->createLogsRequest($entries);
        $metadata = $this->buildMetadata();

        [$result, $status] = $this->getLogsClient()->Export($request, $metadata)->wait();

        $this->checkStatus($status, 'logs');
    }

    /**
     * @param array<Metric> $metrics
     */
    public function sendMetrics(array $metrics) : void
    {
        $request = $this->serializer->createMetricsRequest($metrics);
        $metadata = $this->buildMetadata();

        [$result, $status] = $this->getMetricsClient()->Export($request, $metadata)->wait();

        $this->checkStatus($status, 'metrics');
    }

    /**
     * @param array<Span> $spans
     */
    public function sendSpans(array $spans) : void
    {
        $request = $this->serializer->createSpansRequest($spans);
        $metadata = $this->buildMetadata();

        [$result, $status] = $this->getTracesClient()->Export($request, $metadata)->wait();

        $this->checkStatus($status, 'traces');
    }

    public function shutdown() : void
    {
        if ($this->tracesClient !== null) {
            $this->tracesClient->close();
            $this->tracesClient = null;
        }

        if ($this->metricsClient !== null) {
            $this->metricsClient->close();
            $this->metricsClient = null;
        }

        if ($this->logsClient !== null) {
            $this->logsClient->close();
            $this->logsClient = null;
        }
    }

    /**
     * @return array<string, array<string>>
     */
    private function buildMetadata() : array
    {
        $metadata = [];

        foreach ($this->headers as $key => $value) {
            $metadata[\strtolower($key)] = [$value];
        }

        return $metadata;
    }

    private function checkStatus(object $status, string $signalName) : void
    {
        $statusCode = $status->code ?? -1;
        $statusDetails = $status->details ?? 'Unknown error';

        if ($statusCode !== 0) {
            throw new TransportException(
                \sprintf(
                    'gRPC export failed for %s: %s (code: %d)',
                    $signalName,
                    $statusDetails,
                    $statusCode
                )
            );
        }
    }

    private function createChannel() : mixed
    {
        if ($this->insecure) {
            return ChannelCredentials::createInsecure();
        }

        return ChannelCredentials::createSsl();
    }

    private function getLogsClient() : LogsServiceClient
    {
        if ($this->logsClient === null) {
            $this->logsClient = new LogsServiceClient(
                $this->endpoint,
                ['credentials' => $this->createChannel()]
            );
        }

        return $this->logsClient;
    }

    private function getMetricsClient() : MetricsServiceClient
    {
        if ($this->metricsClient === null) {
            $this->metricsClient = new MetricsServiceClient(
                $this->endpoint,
                ['credentials' => $this->createChannel()]
            );
        }

        return $this->metricsClient;
    }

    private function getTracesClient() : TraceServiceClient
    {
        if ($this->tracesClient === null) {
            $this->tracesClient = new TraceServiceClient(
                $this->endpoint,
                ['credentials' => $this->createChannel()]
            );
        }

        return $this->tracesClient;
    }
}
