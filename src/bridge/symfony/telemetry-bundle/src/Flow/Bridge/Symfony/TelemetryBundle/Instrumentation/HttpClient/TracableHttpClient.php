<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpClient;

use DateTimeImmutable;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Symfony\Contracts\HttpClient\HttpClientInterface;
use Symfony\Contracts\HttpClient\ResponseInterface;
use Symfony\Contracts\HttpClient\ResponseStreamInterface;
use Throwable;

use function parse_url;

final readonly class TracableHttpClient implements HttpClientInterface
{
    public function __construct(
        private HttpClientInterface $client,
        private Telemetry $telemetry,
        private string $clientName,
    ) {}

    /**
     * @param array<array-key, mixed> $options
     */
    public function request(string $method, string $url, array $options = []): ResponseInterface
    {
        $parsedUrl = parse_url($url);
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

        $span = $tracer->span("{$method} {$host}", SpanKind::CLIENT, $attributes);

        try {
            $response = $this->client->request($method, $url, $options);
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));
            $tracer->complete($span);

            throw $exception;
        }

        return new TraceableResponse($tracer, $response, $span);
    }

    public function stream(ResponseInterface|iterable $responses, ?float $timeout = null): ResponseStreamInterface
    {
        if ($responses instanceof ResponseInterface) {
            $responses = [$responses];
        }

        return new ResponseStream(TraceableResponse::stream($this->client, $responses, $timeout));
    }

    /**
     * @param array<array-key, mixed> $options
     */
    public function withOptions(array $options): static
    {
        return new self($this->client->withOptions($options), $this->telemetry, $this->clientName);
    }
}
