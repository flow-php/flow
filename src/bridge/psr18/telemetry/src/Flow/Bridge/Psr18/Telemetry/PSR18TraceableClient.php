<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr18\Telemetry;

use Flow\Telemetry\{PackageVersion, Telemetry};
use Flow\Telemetry\Tracer\{SpanKind, SpanStatus, Tracer};
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\{RequestInterface, ResponseInterface};

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

    public function sendRequest(RequestInterface $request) : ResponseInterface
    {
        $uri = $request->getUri();
        $method = $request->getMethod();
        $scheme = $uri->getScheme() ?: 'http';
        $host = $uri->getHost() ?: 'unknown';
        $url = (string) $uri;

        $span = $this->tracer->span(
            "{$method} {$host}",
            SpanKind::CLIENT,
            [
                'http.method' => $method,
                'http.url' => $url,
                'http.scheme' => $scheme,
                'http.host' => $host,
            ]
        );

        try {
            $response = $this->client->sendRequest($request);

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
            $this->tracer->complete($span);
        }
    }
}
