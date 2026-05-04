<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Transport;

use Flow\Bridge\Telemetry\OTLP\Serializer\GrpcSerializer;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Transport\{Transport, TransportException};
use Google\Protobuf\Internal\Message;
use Grpc\{ChannelCredentials, UnaryCall};
use Opentelemetry\Proto\Collector\Logs\V1\LogsServiceClient;
use Opentelemetry\Proto\Collector\Metrics\V1\MetricsServiceClient;
use Opentelemetry\Proto\Collector\Trace\V1\TraceServiceClient;

/**
 * Asynchronous gRPC transport for OTLP using the grpc PHP extension.
 *
 * Sends are non-blocking: each Export() returns a UnaryCall whose wait()
 * is deferred until shutdown(). Requires the grpc PHP extension and
 * google/protobuf package.
 *
 * Example usage:
 * ```php
 * $transport = new GrpcTransport(
 *     endpoint: 'localhost:4317',
 *     serializer: new ProtobufSerializer(),
 * );
 *
 * $transport->sendSpans($spans);
 * $transport->sendMetrics($metrics);
 *
 * // Block until all pending calls complete
 * $transport->shutdown();
 * ```
 */
final class GrpcTransport implements Transport
{
    private bool $isShutdown = false;

    private ?LogsServiceClient $logsClient = null;

    private ?MetricsServiceClient $metricsClient = null;

    /** @var list<UnaryCall<covariant Message>> */
    private array $pendingCalls = [];

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
        if ($this->isShutdown) {
            throw new TransportException('Cannot send after shutdown');
        }

        $this->pendingCalls[] = $this->getLogsClient()->Export(
            $this->serializer->createLogsRequest($entries),
            $this->buildMetadata(),
        );
    }

    /**
     * @param array<Metric> $metrics
     */
    public function sendMetrics(array $metrics) : void
    {
        if ($this->isShutdown) {
            throw new TransportException('Cannot send after shutdown');
        }

        $this->pendingCalls[] = $this->getMetricsClient()->Export(
            $this->serializer->createMetricsRequest($metrics),
            $this->buildMetadata(),
        );
    }

    /**
     * @param array<Span> $spans
     */
    public function sendSpans(array $spans) : void
    {
        if ($this->isShutdown) {
            throw new TransportException('Cannot send after shutdown');
        }

        $this->pendingCalls[] = $this->getTracesClient()->Export(
            $this->serializer->createSpansRequest($spans),
            $this->buildMetadata(),
        );
    }

    public function shutdown() : void
    {
        if ($this->isShutdown) {
            return;
        }

        $this->isShutdown = true;

        foreach ($this->pendingCalls as $call) {
            $call->wait();
        }

        $this->pendingCalls = [];

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
