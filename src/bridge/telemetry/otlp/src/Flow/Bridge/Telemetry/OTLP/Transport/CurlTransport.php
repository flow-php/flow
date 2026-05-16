<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Transport;

use Flow\Bridge\Telemetry\OTLP\Serializer\JsonSerializer;
use Flow\Bridge\Telemetry\OTLP\Serializer\ProtobufSerializer;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Signal\SignalType;

/**
 * Asynchronous HTTP transport for OTLP using curl_multi for non-blocking I/O.
 * Requests are queued and executed asynchronously, with results processed on
 * subsequent send() calls or on shutdown().
 *
 * When $failover is set, prior failed batches are forwarded to it on the next
 * send()/shutdown(); a FailoverTransportException is then thrown.
 */
final class CurlTransport implements Transport
{
    /** @var list<array{primary: \Throwable, failover: null|\Throwable}> */
    private array $deferredFailures = [];

    /** @var list<\Throwable> */
    private array $failures = [];

    private bool $isShutdown = false;

    private readonly \CurlMultiHandle $multiHandle;

    /** @var array<int, array{handle: \CurlHandle, signals: ?Signals}> */
    private array $pending = [];

    public function __construct(
        private readonly string $endpoint,
        private readonly JsonSerializer|ProtobufSerializer $serializer = new JsonSerializer(),
        private readonly CurlTransportOptions $options = new CurlTransportOptions(),
        private readonly ?Transport $failover = null,
    ) {
        if (!\extension_loaded('curl')) {
            throw new \RuntimeException('ext-curl is required for CurlTransport');
        }

        $this->multiHandle = \curl_multi_init();
    }

    public function send(Signals $signal): void
    {
        [$path, $body, $signalName] = match ($signal->type) {
            SignalType::LOGS => ['/v1/logs', $this->serializer->serializeLogs($signal->allLogs()), 'logs'],
            SignalType::METRICS => [
                '/v1/metrics',
                $this->serializer->serializeMetrics($signal->allMetrics()),
                'metrics',
            ],
            SignalType::TRACES => ['/v1/traces', $this->serializer->serializeSpans($signal->allSpans()), 'traces'],
        };

        if ($this->failover !== null) {
            $this->drainCompleted();
        }

        $this->dispatch($path, $body, $signalName, $this->failover !== null ? $signal : null);

        if ($this->failover !== null && $this->deferredFailures !== []) {
            $snapshot = $this->deferredFailures;
            $this->deferredFailures = [];

            throw new FailoverTransportException($snapshot);
        }
    }

    public function shutdown(): void
    {
        if ($this->isShutdown) {
            return;
        }

        $this->isShutdown = true;

        $shutdownTimeoutMs = $this->options->shutdownTimeoutMs();
        $shutdownDeadlineMicrotime = \microtime(true) + ($shutdownTimeoutMs / 1000);

        // Extend per-handle deadline so slow-but-eventually-succeeds requests get the
        // longer drain budget; the wall-clock cap below still bounds total shutdown time.
        foreach ($this->pending as $entry) {
            \curl_setopt($entry['handle'], \CURLOPT_TIMEOUT_MS, $shutdownTimeoutMs);
        }

        $this->waitForCompletion($shutdownDeadlineMicrotime);

        if ($this->failover === null) {
            $this->processCompleted();
            $this->markStillPendingAsShutdownTimedOut();
        } else {
            $this->drainCompleted();
            $this->forwardStillPendingAsShutdownTimedOut();
        }

        \curl_multi_close($this->multiHandle);

        $cascadeException = null;

        if ($this->failover !== null) {
            try {
                $this->failover->shutdown();
            } catch (\Throwable $e) {
                $cascadeException = $e;
            }
        }

        if ($this->deferredFailures !== []) {
            $snapshot = $this->deferredFailures;
            $this->deferredFailures = [];

            throw new FailoverTransportException($snapshot);
        }

        if ($cascadeException !== null) {
            throw new TransportException(
                \sprintf('OTLP curl shutdown: failover shutdown failed: %s', $cascadeException->getMessage()),
                0,
                $cascadeException,
            );
        }

        if (\count($this->failures) === 0) {
            return;
        }

        $first = $this->failures[0];
        $count = \count($this->failures);
        $message = $count === 1
            ? \sprintf('OTLP curl shutdown: 1 export failed: %s', $first->getMessage())
            : \sprintf('OTLP curl shutdown: %d exports failed; first error: %s', $count, $first->getMessage());

        throw new TransportException($message, 0, $first);
    }

    /**
     * @return list<string>
     */
    private function buildHeaders(): array
    {
        $headers = [
            'Content-Type: ' . match (true) {
                $this->serializer instanceof JsonSerializer => 'application/json',
                $this->serializer instanceof ProtobufSerializer => 'application/x-protobuf',
            },
        ];

        foreach ($this->options->headers() as $name => $value) {
            $headers[] = $name . ': ' . $value;
        }

        return $headers;
    }

    private function buildShutdownTimeoutError(): TransportException
    {
        return new TransportException(\sprintf(
            'OTLP curl shutdown: request still pending when configured shutdown_timeout=%dms expired',
            $this->options->shutdownTimeoutMs(),
        ));
    }

    private function dispatch(string $path, string $body, string $signalName, ?Signals $signalsToRetain): void
    {
        if ($this->isShutdown) {
            throw new TransportException('Cannot send after shutdown');
        }

        $url = \rtrim($this->endpoint, '/') . $path;

        $ch = \curl_init();

        if ($ch === false) {
            throw new TransportException(\sprintf('Failed to initialize curl handle for %s', $signalName));
        }

        \curl_setopt_array($ch, $this->options->toCurlOptions($url, $body, $this->buildHeaders()));

        $result = \curl_multi_add_handle($this->multiHandle, $ch);

        if ($result !== CURLM_OK) {
            \curl_close($ch);

            throw new TransportException(\sprintf(
                'Failed to add curl handle for %s: %s',
                $signalName,
                \curl_multi_strerror($result),
            ));
        }

        $this->pending[(int) $ch] = ['handle' => $ch, 'signals' => $signalsToRetain];

        if ($this->failover === null) {
            $this->processCompleted();
        }
    }

    private function drainCompleted(): void
    {
        foreach ($this->iterateCompleted() as $item) {
            if ($item['primaryError'] === null) {
                continue;
            }

            $failoverError = null;

            if ($item['entry'] !== null && $item['entry']['signals'] !== null && $this->failover !== null) {
                try {
                    $this->failover->send($item['entry']['signals']);
                } catch (\Throwable $e) {
                    $failoverError = $e;
                }
            }

            $this->deferredFailures[] = ['primary' => $item['primaryError'], 'failover' => $failoverError];
        }
    }

    private function forwardStillPendingAsShutdownTimedOut(): void
    {
        if ($this->failover === null) {
            return;
        }

        foreach ($this->iterateStillPending() as $item) {
            $failoverError = null;

            if ($item['entry']['signals'] !== null) {
                try {
                    $this->failover->send($item['entry']['signals']);
                } catch (\Throwable $e) {
                    $failoverError = $e;
                }
            }

            $this->deferredFailures[] = ['primary' => $item['primaryError'], 'failover' => $failoverError];
        }
    }

    /**
     * @return \Generator<int, array{primaryError: ?TransportException, entry: ?array{handle: \CurlHandle, signals: ?Signals}}>
     */
    private function iterateCompleted(): \Generator
    {
        $running = 0;
        \curl_multi_exec($this->multiHandle, $running);

        while ($info = \curl_multi_info_read($this->multiHandle)) {
            /** @var \CurlHandle $ch */
            $ch = $info['handle'];
            $id = (int) $ch;

            $errno = $info['result'];
            $httpCode = (int) \curl_getinfo($ch, \CURLINFO_HTTP_CODE);
            $effectiveUrl = (string) \curl_getinfo($ch, \CURLINFO_EFFECTIVE_URL);
            $urlForMessage = $effectiveUrl !== '' ? $effectiveUrl : 'unknown url';
            $totalTimeMs = (int) \round((float) \curl_getinfo($ch, \CURLINFO_TOTAL_TIME) * 1000);
            $connectTimeMs = (int) \round((float) \curl_getinfo($ch, \CURLINFO_CONNECT_TIME) * 1000);

            $primaryError = null;

            if ($errno !== \CURLE_OK) {
                $primaryError = new TransportException(\sprintf(
                    'curl error %d (%s) for %s; elapsed connect=%dms total=%dms (configured connect_timeout=%dms, timeout=%dms)',
                    $errno,
                    \curl_strerror($errno) ?? 'unknown',
                    $urlForMessage,
                    $connectTimeMs,
                    $totalTimeMs,
                    $this->options->connectTimeoutMs(),
                    $this->options->timeoutMs(),
                ));
            } elseif ($httpCode < 200 || $httpCode >= 300) {
                $primaryError = new TransportException(\sprintf(
                    'HTTP %d from %s after %dms',
                    $httpCode,
                    $urlForMessage,
                    $totalTimeMs,
                ));
            }

            $entry = $this->pending[$id] ?? null;

            \curl_multi_remove_handle($this->multiHandle, $ch);
            unset($this->pending[$id]);

            yield ['primaryError' => $primaryError, 'entry' => $entry];
        }
    }

    /**
     * @return \Generator<int, array{primaryError: TransportException, entry: array{handle: \CurlHandle, signals: ?Signals}}>
     */
    private function iterateStillPending(): \Generator
    {
        foreach ($this->pending as $id => $entry) {
            \curl_multi_remove_handle($this->multiHandle, $entry['handle']);
            unset($this->pending[$id]);

            yield ['primaryError' => $this->buildShutdownTimeoutError(), 'entry' => $entry];
        }
    }

    private function markStillPendingAsShutdownTimedOut(): void
    {
        foreach ($this->iterateStillPending() as $item) {
            $this->failures[] = $item['primaryError'];
        }
    }

    private function processCompleted(): void
    {
        foreach ($this->iterateCompleted() as $item) {
            if ($item['primaryError'] !== null) {
                $this->failures[] = $item['primaryError'];
            }
        }
    }

    private function waitForCompletion(?float $deadlineMicrotime = null): void
    {
        if (\count($this->pending) === 0) {
            return;
        }

        $running = 0;

        do {
            $status = \curl_multi_exec($this->multiHandle, $running);
        } while ($status === CURLM_CALL_MULTI_PERFORM);

        while ($running > 0 && $status === CURLM_OK) {
            $selectTimeout = 1.0;

            if ($deadlineMicrotime !== null) {
                $remaining = $deadlineMicrotime - \microtime(true);

                if ($remaining <= 0.0) {
                    return;
                }

                $selectTimeout = \min(1.0, $remaining);
            }

            if (\curl_multi_select($this->multiHandle, $selectTimeout) === -1) {
                \usleep(1000);
            }

            do {
                $status = \curl_multi_exec($this->multiHandle, $running);
            } while ($status === CURLM_CALL_MULTI_PERFORM);
        }
    }
}
