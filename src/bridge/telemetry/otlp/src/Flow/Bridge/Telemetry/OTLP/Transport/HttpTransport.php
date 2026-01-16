<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Transport;

use Flow\Bridge\Telemetry\OTLP\Serializer\JsonSerializer;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Serializer\Serializer;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Transport\{Transport, TransportException};
use Psr\Http\Client\{ClientExceptionInterface, ClientInterface};
use Psr\Http\Message\{RequestFactoryInterface, StreamFactoryInterface};

/**
 * HTTP transport for OTLP using PSR-18 HTTP client.
 *
 * Sends telemetry data over HTTP to OTLP-compatible endpoints.
 * Supports both JSON and Protobuf formats via the Serializer interface.
 *
 * Example usage:
 * ```php
 * $transport = new HttpTransport(
 *     httpClient: $psrHttpClient,
 *     requestFactory: $psrRequestFactory,
 *     streamFactory: $psrStreamFactory,
 *     endpoint: 'http://localhost:4318',
 *     serializer: new JsonSerializer(),
 * );
 *
 * $transport->sendSpans($spans);
 * ```
 */
final readonly class HttpTransport implements Transport
{
    /**
     * @param ClientInterface $httpClient PSR-18 HTTP client
     * @param RequestFactoryInterface $requestFactory PSR-17 request factory
     * @param StreamFactoryInterface $streamFactory PSR-17 stream factory
     * @param string $endpoint Base OTLP HTTP endpoint URL (e.g., 'http://localhost:4318')
     * @param Serializer $serializer Serializer for encoding telemetry data
     * @param array<string, string> $headers Additional headers to include in requests
     */
    public function __construct(
        private ClientInterface $httpClient,
        private RequestFactoryInterface $requestFactory,
        private StreamFactoryInterface $streamFactory,
        private string $endpoint,
        private Serializer $serializer,
        private array $headers = [],
    ) {
    }

    /**
     * @param array<LogEntry> $entries
     */
    public function sendLogs(array $entries) : void
    {
        $this->send('/v1/logs', $this->serializer->serializeLogs($entries), 'logs');
    }

    /**
     * @param array<Metric> $metrics
     */
    public function sendMetrics(array $metrics) : void
    {
        $this->send('/v1/metrics', $this->serializer->serializeMetrics($metrics), 'metrics');
    }

    /**
     * @param array<Span> $spans
     */
    public function sendSpans(array $spans) : void
    {
        $this->send('/v1/traces', $this->serializer->serializeSpans($spans), 'traces');
    }

    public function shutdown() : void
    {
    }

    private function send(string $path, string $body, string $signalName) : void
    {
        $url = \rtrim($this->endpoint, '/') . $path;

        $request = $this->requestFactory->createRequest('POST', $url)
            ->withHeader('Content-Type', $this->serializer instanceof JsonSerializer ? 'application/json' : 'application/x-protobuf');

        foreach ($this->headers as $name => $value) {
            $request = $request->withHeader($name, $value);
        }

        $request = $request->withBody($this->streamFactory->createStream($body));

        try {
            $response = $this->httpClient->sendRequest($request);
        } catch (ClientExceptionInterface $e) {
            throw new TransportException(
                \sprintf('Failed to send %s to OTLP endpoint: %s', $signalName, $e->getMessage()),
                previous: $e
            );
        }

        $statusCode = $response->getStatusCode();

        if ($statusCode >= 400) {
            $responseBody = (string) $response->getBody();

            throw new TransportException(
                \sprintf(
                    'OTLP endpoint returned HTTP %d for %s: %s',
                    $statusCode,
                    $signalName,
                    $responseBody
                )
            );
        }
    }
}
