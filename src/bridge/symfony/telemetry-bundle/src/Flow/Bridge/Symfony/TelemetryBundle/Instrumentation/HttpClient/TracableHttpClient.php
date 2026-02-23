<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpClient;

use Flow\Telemetry\{PackageVersion, Telemetry};
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
        $port = $parsedUrl['port'] ?? null;

        $tracer = $this->telemetry->tracer('flow.symfony.http_client', PackageVersion::get('symfony/http-client'));

        $attributes = [
            'http.request.method' => $method,
            'url.full' => $url,
            'url.scheme' => $scheme,
            'server.address' => $host,
            'http.client.name' => $this->clientName,
        ];

        if ($port !== null && $port !== 80 && $port !== 443) {
            $attributes['server.port'] = $port;
        }

        $span = $tracer->span(
            "{$method} {$host}",
            SpanKind::CLIENT,
            $attributes
        );

        try {
            $response = $this->client->request($method, $url, $options);

            $statusCode = $response->getStatusCode();
            $span->setAttribute('http.response.status_code', $statusCode);

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
