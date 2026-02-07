<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Telemetry\HttpClient;

use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\{SpanKind, SpanStatus};
use Symfony\Contracts\HttpClient\{HttpClientInterface, ResponseInterface, ResponseStreamInterface};

final readonly class TracableHttpClient implements HttpClientInterface
{
    public function __construct(
        private HttpClientInterface $client,
        private Telemetry $telemetry,
        private string $clientName,
    ) {
    }

    /**
     * @param array<string, mixed> $options
     */
    public function request(string $method, string $url, array $options = []) : ResponseInterface
    {
        $parsedUrl = \parse_url($url);
        $scheme = $parsedUrl['scheme'] ?? 'http';
        $host = $parsedUrl['host'] ?? 'unknown';

        $tracer = $this->telemetry->tracer('flow.symfony.http_client');

        $span = $tracer->span(
            "{$method} {$host}",
            SpanKind::CLIENT,
            [
                'http.method' => $method,
                'http.url' => $url,
                'http.scheme' => $scheme,
                'http.host' => $host,
                'http.client.name' => $this->clientName,
            ]
        );

        try {
            $response = $this->client->request($method, $url, $options);

            $statusCode = $response->getStatusCode();
            $span->setAttribute('http.status_code', $statusCode);

            if ($statusCode >= 400) {
                $span->setStatus(SpanStatus::error("HTTP {$statusCode}"));
            } else {
                $span->setStatus(SpanStatus::ok());
            }

            return $response;
        } catch (\Throwable $exception) {
            $span->recordException($exception, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $tracer->complete($span);
        }
    }

    public function stream(ResponseInterface|iterable $responses, ?float $timeout = null) : ResponseStreamInterface
    {
        return $this->client->stream($responses, $timeout);
    }

    /**
     * @param array<string, mixed> $options
     */
    public function withOptions(array $options) : static
    {
        return new self(
            $this->client->withOptions($options),
            $this->telemetry,
            $this->clientName
        );
    }
}
