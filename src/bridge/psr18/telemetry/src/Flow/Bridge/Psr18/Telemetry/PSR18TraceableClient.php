<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr18\Telemetry;

use DateTimeImmutable;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\SemConvAttributes;
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
            SemConvAttributes::HTTP_REQUEST_METHOD => $method,
            SemConvAttributes::URL_FULL => $url,
            SemConvAttributes::URL_SCHEME => $scheme,
            SemConvAttributes::SERVER_ADDRESS => $host,
            // OTEL HTTP semconv: server.port is Required on client spans; fall back to the
            // scheme default when the URI carries no explicit port.
            SemConvAttributes::SERVER_PORT => $port ?? ($scheme === 'https' ? 443 : 80),
        ];

        // OTEL HTTP semconv: client span name is "{method}" - host would be per-host cardinality.
        $span = $this->tracer->span($method, SpanKind::CLIENT, $attributes);
        $scope = $this->tracer->activate($span);

        try {
            $response = $this->client->sendRequest($request);

            $statusCode = $response->getStatusCode();
            $span->setAttribute(SemConvAttributes::HTTP_RESPONSE_STATUS_CODE, $statusCode);

            // OTEL HTTP semconv: for SpanKind.CLIENT both 4xx and 5xx are Errors; 1xx-3xx leaves status unset.
            if ($statusCode >= 400) {
                $span->setStatus(SpanStatus::error("HTTP {$statusCode}"));
                $span->setAttribute(SemConvAttributes::ERROR_TYPE, (string) $statusCode);
            }

            return $response;
        } catch (Throwable $exception) {
            $span->recordException($exception, new DateTimeImmutable());
            $span->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class);
            $span->setStatus(SpanStatus::error($exception->getMessage()));

            throw $exception;
        } finally {
            $scope->detach();
            $this->tracer->complete($span);
        }
    }
}
