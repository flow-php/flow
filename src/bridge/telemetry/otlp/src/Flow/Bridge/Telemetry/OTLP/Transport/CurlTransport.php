<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Transport;

use Flow\Bridge\Telemetry\OTLP\Serializer\JsonSerializer;
use Flow\Telemetry\Serializer\Serializer;
use Flow\Telemetry\Signal\{SignalType, Signals};

/**
 * Asynchronous HTTP transport for OTLP using curl_multi for non-blocking I/O.
 * Requests are queued and executed asynchronously, with results processed on
 * subsequent send() calls or on shutdown().
 */
final class CurlTransport implements Transport
{
    /** @var list<string> */
    private array $failures = [];

    private bool $isShutdown = false;

    private readonly \CurlMultiHandle $multiHandle;

    /** @var array<int, \CurlHandle> */
    private array $pendingHandles = [];

    /**
     * @param string $endpoint Base OTLP HTTP endpoint URL (e.g., 'http://localhost:4318')
     * @param Serializer $serializer Serializer for encoding telemetry data
     * @param CurlTransportOptions $options Transport configuration options
     */
    public function __construct(
        private readonly string $endpoint,
        private readonly Serializer $serializer,
        private readonly CurlTransportOptions $options = new CurlTransportOptions(),
    ) {
        if (!\extension_loaded('curl')) {
            throw new \RuntimeException('ext-curl is required for CurlTransport');
        }

        $this->multiHandle = \curl_multi_init();
    }

    public function send(Signals $signal) : void
    {
        [$path, $body, $signalName] = match ($signal->type) {
            SignalType::LOGS => ['/v1/logs', $this->serializer->serializeLogs($signal->allLogs()), 'logs'],
            SignalType::METRICS => ['/v1/metrics', $this->serializer->serializeMetrics($signal->allMetrics()), 'metrics'],
            SignalType::TRACES => ['/v1/traces', $this->serializer->serializeSpans($signal->allSpans()), 'traces'],
        };

        $this->dispatch($path, $body, $signalName);
    }

    public function shutdown() : void
    {
        if ($this->isShutdown) {
            return;
        }

        $this->isShutdown = true;

        $this->waitForCompletion();

        $this->processCompleted();

        \curl_multi_close($this->multiHandle);

        if (\count($this->failures) === 0) {
            return;
        }

        $first = $this->failures[0];
        $count = \count($this->failures);
        $message = $count === 1
            ? \sprintf('OTLP curl shutdown: 1 export failed: %s', $first)
            : \sprintf('OTLP curl shutdown: %d exports failed; first error: %s', $count, $first);

        throw new TransportException($message);
    }

    /**
     * @return list<string>
     */
    private function buildHeaders() : array
    {
        $headers = [
            'Content-Type: ' . ($this->serializer instanceof JsonSerializer ? 'application/json' : 'application/x-protobuf'),
        ];

        foreach ($this->options->headers() as $name => $value) {
            $headers[] = $name . ': ' . $value;
        }

        return $headers;
    }

    private function dispatch(string $path, string $body, string $signalName) : void
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

            throw new TransportException(\sprintf('Failed to add curl handle for %s: %s', $signalName, \curl_multi_strerror($result)));
        }

        $this->pendingHandles[(int) $ch] = $ch;

        $this->processCompleted();
    }

    private function processCompleted() : void
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

            if ($errno !== \CURLE_OK) {
                $this->failures[] = \sprintf(
                    'curl error %d (%s) for %s',
                    $errno,
                    \curl_strerror($errno) ?? 'unknown',
                    $urlForMessage,
                );
            } elseif ($httpCode < 200 || $httpCode >= 300) {
                $this->failures[] = \sprintf('HTTP %d from %s', $httpCode, $urlForMessage);
            }

            \curl_multi_remove_handle($this->multiHandle, $ch);
            \curl_close($ch);
            unset($this->pendingHandles[$id]);
        }
    }

    private function waitForCompletion() : void
    {
        if (\count($this->pendingHandles) === 0) {
            return;
        }

        $running = 0;

        do {
            $status = \curl_multi_exec($this->multiHandle, $running);
        } while ($status === CURLM_CALL_MULTI_PERFORM);

        while ($running > 0 && $status === CURLM_OK) {
            if (\curl_multi_select($this->multiHandle, 1.0) === -1) {
                \usleep(1000);
            }

            do {
                $status = \curl_multi_exec($this->multiHandle, $running);
            } while ($status === CURLM_CALL_MULTI_PERFORM);
        }
    }
}
