<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Transport;

use const Grpc\{STATUS_ABORTED, STATUS_ALREADY_EXISTS, STATUS_CANCELLED, STATUS_DATA_LOSS, STATUS_DEADLINE_EXCEEDED, STATUS_FAILED_PRECONDITION, STATUS_INTERNAL, STATUS_INVALID_ARGUMENT, STATUS_NOT_FOUND, STATUS_OK, STATUS_OUT_OF_RANGE, STATUS_PERMISSION_DENIED, STATUS_RESOURCE_EXHAUSTED, STATUS_UNAUTHENTICATED, STATUS_UNAVAILABLE, STATUS_UNIMPLEMENTED, STATUS_UNKNOWN};
use Flow\Bridge\Telemetry\OTLP\Serializer\{GrpcRequestFactory, ProtobufSerializer};
use Flow\Telemetry\Signal\{SignalType, Signals};
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

    private readonly GrpcRequestFactory $requestFactory;

    private ?TraceServiceClient $tracesClient = null;

    /**
     * @param string $endpoint gRPC endpoint (e.g., 'localhost:4317')
     * @param array<string, string> $headers Additional headers (metadata) to include in requests
     * @param bool $insecure Whether to use insecure channel credentials (default true for local dev)
     */
    public function __construct(
        private readonly string $endpoint,
        private readonly array $headers = [],
        private readonly bool $insecure = true,
    ) {
        if (!\extension_loaded('grpc')) {
            throw new \RuntimeException(
                'The grpc PHP extension is required for GrpcTransport. '
                . 'Install it via: pecl install grpc'
            );
        }

        $this->requestFactory = new ProtobufSerializer();
    }

    public function send(Signals $signal) : void
    {
        if ($this->isShutdown) {
            throw new TransportException('Cannot send after shutdown');
        }

        $this->pendingCalls[] = match ($signal->type) {
            SignalType::LOGS => $this->getLogsClient()->Export(
                $this->requestFactory->createLogsRequest($signal->allLogs()),
                $this->buildMetadata(),
            ),
            SignalType::METRICS => $this->getMetricsClient()->Export(
                $this->requestFactory->createMetricsRequest($signal->allMetrics()),
                $this->buildMetadata(),
            ),
            SignalType::TRACES => $this->getTracesClient()->Export(
                $this->requestFactory->createSpansRequest($signal->allSpans()),
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

        /** @var list<\Throwable> $failures */
        $failures = [];

        foreach ($this->pendingCalls as $call) {
            try {
                [, $status] = $call->wait();
            } catch (\Throwable $e) {
                $failures[] = $e;

                continue;
            }

            if ($status->code !== STATUS_OK) {
                $failures[] = new TransportException(\sprintf(
                    'gRPC status %d (%s): %s',
                    $status->code,
                    self::grpcStatusName($status->code),
                    $status->details ?? '',
                ));
            }
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

        if (\count($failures) === 0) {
            return;
        }

        $first = $failures[0];
        $count = \count($failures);
        $message = $count === 1
            ? \sprintf('OTLP gRPC shutdown: 1 export failed: %s', $first->getMessage())
            : \sprintf('OTLP gRPC shutdown: %d exports failed; first error: %s', $count, $first->getMessage());

        throw new TransportException($message, 0, $first);
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

    private static function grpcStatusName(int $code) : string
    {
        return match ($code) {
            STATUS_OK => 'OK',
            STATUS_CANCELLED => 'CANCELLED',
            STATUS_UNKNOWN => 'UNKNOWN',
            STATUS_INVALID_ARGUMENT => 'INVALID_ARGUMENT',
            STATUS_DEADLINE_EXCEEDED => 'DEADLINE_EXCEEDED',
            STATUS_NOT_FOUND => 'NOT_FOUND',
            STATUS_ALREADY_EXISTS => 'ALREADY_EXISTS',
            STATUS_PERMISSION_DENIED => 'PERMISSION_DENIED',
            STATUS_UNAUTHENTICATED => 'UNAUTHENTICATED',
            STATUS_RESOURCE_EXHAUSTED => 'RESOURCE_EXHAUSTED',
            STATUS_FAILED_PRECONDITION => 'FAILED_PRECONDITION',
            STATUS_ABORTED => 'ABORTED',
            STATUS_OUT_OF_RANGE => 'OUT_OF_RANGE',
            STATUS_UNIMPLEMENTED => 'UNIMPLEMENTED',
            STATUS_INTERNAL => 'INTERNAL',
            STATUS_UNAVAILABLE => 'UNAVAILABLE',
            STATUS_DATA_LOSS => 'DATA_LOSS',
            default => 'UNKNOWN',
        };
    }
}
