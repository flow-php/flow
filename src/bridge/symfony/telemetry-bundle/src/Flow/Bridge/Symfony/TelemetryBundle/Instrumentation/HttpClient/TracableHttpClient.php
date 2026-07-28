<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpClient;

use DateTimeImmutable;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\SemConvAttributes;
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
            SemConvAttributes::HTTP_REQUEST_METHOD => $method,
            SemConvAttributes::URL_FULL => $url,
            SemConvAttributes::URL_SCHEME => $scheme,
            SemConvAttributes::SERVER_ADDRESS => $host,
            HttpClientAttributes::ATTR_CLIENT_NAME => $this->clientName,
            // OTEL HTTP semconv: server.port is Required on client spans; fall back to the
            // scheme default when the URL carries no explicit port.
            SemConvAttributes::SERVER_PORT => $port ?? ($scheme === 'https' ? 443 : 80),
        ];

        // OTEL HTTP semconv: client span name is "{method}" - host would be per-host cardinality.
        $span = $tracer->span($method, SpanKind::CLIENT, $attributes);
        $scope = $tracer->activate($span);

        try {
            $response = $this->client->request($method, $url, $options);
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setStatus(SpanStatus::error($exception->getMessage()));
            $scope->detach();
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
