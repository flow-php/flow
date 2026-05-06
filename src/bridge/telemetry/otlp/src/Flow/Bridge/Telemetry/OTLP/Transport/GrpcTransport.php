<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Transport;

use Flow\Bridge\Telemetry\OTLP\Serializer\GrpcSerializer;
use Flow\Telemetry\Signal\{SignalType, Signals};
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

    public function send(Signals $signal) : void
    {
        if ($this->isShutdown) {
            throw new TransportException('Cannot send after shutdown');
        }

        $this->pendingCalls[] = match ($signal->type) {
            SignalType::LOGS => $this->getLogsClient()->Export(
                $this->serializer->createLogsRequest($signal->allLogs()),
                $this->buildMetadata(),
            ),
            SignalType::METRICS => $this->getMetricsClient()->Export(
                $this->serializer->createMetricsRequest($signal->allMetrics()),
                $this->buildMetadata(),
            ),
            SignalType::TRACES => $this->getTracesClient()->Export(
                $this->serializer->createSpansRequest($signal->allSpans()),
                $this->buildMetadata(),
            ),
        };
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
