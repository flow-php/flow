<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr18\Telemetry;

use DateTimeImmutable;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\Tracer;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Throwable;

final readonly class PSR18TraceableClient implements ClientInterface
{
    private Tracer $tracer;

    public function __construct(
        private ClientInterface $client,
        Telemetry $telemetry,
    ) {
        $this->tracer = $telemetry->tracer(
            'flow.psr18.http_client',
            PackageVersion::get('flow-php/psr18-telemetry-bridge'),
        );
    }

    public function sendRequest(RequestInterface $request): ResponseInterface
    {
        $uri = $request->getUri();
        $method = $request->getMethod();
        $scheme = $uri->getScheme() ?: 'http';
        $host = $uri->getHost() ?: 'unknown';
        $port = $uri->getPort();
        $url = (string) $uri;

        $attributes = [
            'http.request.method' => $method,
            'url.full' => $url,
            'url.scheme' => $scheme,
            'server.address' => $host,
        ];

        if ($port !== null && $port !== 80 && $port !== 443) {
            $attributes['server.port'] = $port;
        }

        $span = $this->tracer->span("{$method} {$host}", SpanKind::CLIENT, $attributes);

        try {
            $response = $this->client->sendRequest($request);

            $statusCode = $response->getStatusCode();
            $span->setAttribute('http.response.status_code', $statusCode);

            // OTEL HTTP semconv: for SpanKind.CLIENT both 4xx and 5xx are Errors; 1xx-3xx leaves status unset.
            if ($statusCode >= 400) {
                $span->setStatus(SpanStatus::error("HTTP {$statusCode}"));
                $span->setAttribute('error.type', (string) $statusCode);
            }

            return $response;
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute('error.type', $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $this->tracer->complete($span);
        }
    }
}
