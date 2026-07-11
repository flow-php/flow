<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Transport;

use Flow\Bridge\Telemetry\OTLP\Serializer\JsonSerializer;
use Flow\Bridge\Telemetry\OTLP\Serializer\ProtobufSerializer;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Signal\SignalType;
use RuntimeException;
use Throwable;

use function curl_errno;
use function curl_exec;
use function curl_getinfo;
use function curl_init;
use function curl_setopt_array;
use function curl_strerror;
use function extension_loaded;
use function round;
use function rtrim;
use function sprintf;

use const CURLINFO_CONNECT_TIME;
use const CURLINFO_EFFECTIVE_URL;
use const CURLINFO_HTTP_CODE;
use const CURLINFO_TOTAL_TIME;

/**
 * Synchronous HTTP transport for OTLP.
 */
final class CurlTransport implements Transport
{
    private bool $isShutdown = false;

    public function __construct(
        private readonly string $endpoint,
        private readonly JsonSerializer|ProtobufSerializer $serializer = new JsonSerializer(),
        private readonly CurlTransportOptions $options = new CurlTransportOptions(),
        private readonly ?Transport $failover = null,
    ) {
        if (!extension_loaded('curl')) {
            throw new RuntimeException('ext-curl is required for CurlTransport');
        }
    }

    public function send(Signals $signal): void
    {
        if ($this->isShutdown) {
            throw new TransportException('Cannot send after shutdown');
        }

        if ($signal->count() === 0) {
            return;
        }

        [$path, $body, $signalName] = match ($signal->type) {
            SignalType::LOGS => ['/v1/logs', $this->serializer->serializeLogs($signal->allLogs()), 'logs'],
            SignalType::METRICS => [
                '/v1/metrics',
                $this->serializer->serializeMetrics($signal->allMetrics()),
                'metrics',
            ],
            SignalType::TRACES => ['/v1/traces', $this->serializer->serializeSpans($signal->allSpans()), 'traces'],
        };

        $error = $this->dispatch($path, $body, $signalName);

        if ($error === null) {
            return;
        }

        $failover = $this->failover;

        if ($failover === null) {
            throw $error;
        }

        $failoverError = null;

        try {
            $failover->send($signal);
        } catch (Throwable $e) {
            $failoverError = $e;
        }

        throw new FailoverTransportException([['primary' => $error, 'failover' => $failoverError]]);
    }

    public function shutdown(): void
    {
        if ($this->isShutdown) {
            return;
        }

        $this->isShutdown = true;

        $failover = $this->failover;

        if ($failover === null) {
            return;
        }

        try {
            $failover->shutdown();
        } catch (Throwable $e) {
            throw new TransportException(
                sprintf('OTLP curl shutdown: failover shutdown failed: %s', $e->getMessage()),
                0,
                $e,
            );
        }
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

    private function dispatch(string $path, string $body, string $signalName): ?TransportException
    {
        $url = rtrim($this->endpoint, '/') . $path;

        $ch = curl_init();

        if ($ch === false) {
            return new TransportException(sprintf('Failed to initialize curl handle for %s', $signalName));
        }

        curl_setopt_array($ch, $this->options->toCurlOptions($url, $body, $this->buildHeaders()));

        curl_exec($ch);

        $errno = curl_errno($ch);
        $httpCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $effectiveUrl = (string) curl_getinfo($ch, CURLINFO_EFFECTIVE_URL);
        $urlForMessage = $effectiveUrl !== '' ? $effectiveUrl : $url;
        // @mago-expect analysis:invalid-type-cast(2) -- curl_getinfo stubs return mixed but specific options return float
        $totalTimeMs = (int) round((float) curl_getinfo($ch, CURLINFO_TOTAL_TIME) * 1000);
        $connectTimeMs = (int) round((float) curl_getinfo($ch, CURLINFO_CONNECT_TIME) * 1000);

        if ($errno !== 0) {
            return new TransportException(sprintf(
                'curl error %d (%s) for %s; elapsed connect=%dms total=%dms (configured connect_timeout=%dms, timeout=%dms)',
                $errno,
                curl_strerror($errno) ?? 'unknown',
                $urlForMessage,
                $connectTimeMs,
                $totalTimeMs,
                $this->options->connectTimeoutMs(),
                $this->options->timeoutMs(),
            ));
        }

        if ($httpCode < 200 || $httpCode >= 300) {
            return new TransportException(sprintf(
                'HTTP %d from %s after %dms',
                $httpCode,
                $urlForMessage,
                $totalTimeMs,
            ));
        }

        return null;
    }
}
