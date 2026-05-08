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
 *
 * When $failover is set, prior failed batches are forwarded to it on the next
 * send()/shutdown(); a FailoverTransportException is then thrown.
 */
final class GrpcTransport implements Transport
{
    public const int DEFAULT_SHUTDOWN_TIMEOUT_MS = 5000;

    public const int DEFAULT_TIMEOUT_MS = 250;

    /** @var list<array{primary: \Throwable, failover: null|\Throwable}> */
    private array $deferredFailures = [];

    private bool $isShutdown = false;

    private ?LogsServiceClient $logsClient = null;

    private ?MetricsServiceClient $metricsClient = null;

    /** @var list<array{call: UnaryCall<covariant Message>, signals: ?Signals}> */
    private array $pending = [];

    private readonly GrpcRequestFactory $requestFactory;

    private ?TraceServiceClient $tracesClient = null;

    /**
     * @param string $endpoint gRPC endpoint (e.g., 'localhost:4317')
     * @param array<string, string> $headers Additional headers (metadata) to include in requests
     * @param bool $insecure Whether to use insecure channel credentials (default true for local dev)
     * @param int $timeoutMs Per-call deadline in milliseconds (covers connect + send + receive); gRPC has no separate connect timeout
     * @param int $shutdownTimeoutMs Wall-clock budget for draining pending calls at shutdown; remaining calls are cancelled
     * @param ?Transport $failover Optional failover transport receiving prior batches when primary fails
     */
    public function __construct(
        private readonly string $endpoint,
        private readonly array $headers = [],
        private readonly bool $insecure = true,
        private readonly int $timeoutMs = self::DEFAULT_TIMEOUT_MS,
        private readonly int $shutdownTimeoutMs = self::DEFAULT_SHUTDOWN_TIMEOUT_MS,
        private readonly ?Transport $failover = null,
    ) {
        if (!\extension_loaded('grpc')) {
            throw new \RuntimeException(
                'The grpc PHP extension is required for GrpcTransport. '
                . 'Install it via: pecl install grpc'
            );
        }

        if ($timeoutMs < 0) {
            throw new \InvalidArgumentException('Timeout must be non-negative');
        }

        if ($shutdownTimeoutMs < 0) {
            throw new \InvalidArgumentException('Shutdown timeout must be non-negative');
        }

        $this->requestFactory = new ProtobufSerializer();
    }

    public function send(Signals $signal) : void
    {
        if ($this->isShutdown) {
            throw new TransportException('Cannot send after shutdown');
        }

        if ($this->failover !== null) {
            $this->drainPending();
        }

        $callOptions = ['timeout' => $this->timeoutMs * 1000];

        $call = match ($signal->type) {
            SignalType::LOGS => $this->getLogsClient()->Export(
                $this->requestFactory->createLogsRequest($signal->allLogs()),
                $this->buildMetadata(),
                $callOptions,
            ),
            SignalType::METRICS => $this->getMetricsClient()->Export(
                $this->requestFactory->createMetricsRequest($signal->allMetrics()),
                $this->buildMetadata(),
                $callOptions,
            ),
            SignalType::TRACES => $this->getTracesClient()->Export(
                $this->requestFactory->createSpansRequest($signal->allSpans()),
                $this->buildMetadata(),
                $callOptions,
            ),
        };

        $this->pending[] = ['call' => $call, 'signals' => $this->failover !== null ? $signal : null];

        if ($this->failover !== null && $this->deferredFailures !== []) {
            $snapshot = $this->deferredFailures;
            $this->deferredFailures = [];

            throw new FailoverTransportException($snapshot);
        }
    }

    public function shutdown() : void
    {
        if ($this->isShutdown) {
            return;
        }

        $this->isShutdown = true;

        $shutdownDeadlineMicrotime = \microtime(true) + ($this->shutdownTimeoutMs / 1000);

        if ($this->failover === null) {
            $this->shutdownWithoutFailover($shutdownDeadlineMicrotime);

            return;
        }

        $this->drainPending($shutdownDeadlineMicrotime);

        $this->closeClients();

        $cascadeException = null;

        try {
            $this->failover->shutdown();
        } catch (\Throwable $e) {
            $cascadeException = $e;
        }

        if ($this->deferredFailures !== []) {
            $snapshot = $this->deferredFailures;
            $this->deferredFailures = [];

            throw new FailoverTransportException($snapshot);
        }

        if ($cascadeException !== null) {
            throw new TransportException(
                \sprintf('OTLP gRPC shutdown: failover shutdown failed: %s', $cascadeException->getMessage()),
                0,
                $cascadeException,
            );
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

    private function buildShutdownTimeoutError() : TransportException
    {
        return new TransportException(\sprintf(
            'OTLP gRPC shutdown: call cancelled when configured shutdown_timeout=%dms expired',
            $this->shutdownTimeoutMs,
        ));
    }

    private function closeClients() : void
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

    private function createChannel() : mixed
    {
        if ($this->insecure) {
            return ChannelCredentials::createInsecure();
        }

        return ChannelCredentials::createSsl();
    }

    private function drainPending(?float $deadlineMicrotime = null) : void
    {
        foreach ($this->iteratePending($deadlineMicrotime) as $item) {
            if ($item['primaryError'] === null) {
                continue;
            }

            $failoverError = null;

            if ($item['entry']['signals'] !== null && $this->failover !== null) {
                try {
                    $this->failover->send($item['entry']['signals']);
                } catch (\Throwable $e) {
                    $failoverError = $e;
                }
            }

            $this->deferredFailures[] = ['primary' => $item['primaryError'], 'failover' => $failoverError];
        }
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

    /**
     * @return \Generator<int, array{primaryError: ?\Throwable, entry: array{call: UnaryCall<covariant Message>, signals: ?Signals}}>
     */
    private function iteratePending(?float $deadlineMicrotime = null) : \Generator
    {
        $pending = $this->pending;
        $this->pending = [];

        foreach ($pending as $entry) {
            if ($deadlineMicrotime !== null && \microtime(true) >= $deadlineMicrotime) {
                $entry['call']->cancel();
                yield ['primaryError' => $this->buildShutdownTimeoutError(), 'entry' => $entry];

                continue;
            }

            $primaryError = null;

            try {
                [, $status] = $entry['call']->wait();

                if ($status->code !== STATUS_OK) {
                    $primaryError = new TransportException(\sprintf(
                        'gRPC status %d (%s): %s',
                        $status->code,
                        self::grpcStatusName($status->code),
                        $status->details ?? '',
                    ));
                }
            } catch (\Throwable $e) {
                $primaryError = $e;
            }

            yield ['primaryError' => $primaryError, 'entry' => $entry];
        }
    }

    private function shutdownWithoutFailover(?float $deadlineMicrotime) : void
    {
        /** @var list<\Throwable> $failures */
        $failures = [];

        foreach ($this->iteratePending($deadlineMicrotime) as $item) {
            if ($item['primaryError'] !== null) {
                $failures[] = $item['primaryError'];
            }
        }

        $this->closeClients();

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
