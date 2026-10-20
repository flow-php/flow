<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr18\Telemetry;

use DateTimeImmutable;
use Flow\Telemetry\ClientUrlAttributes;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Sanitizer\UrlSanitizer;
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
    private ClientUrlAttributes $clientUrlAttributes;

    private Tracer $tracer;

    public function __construct(
        private ClientInterface $client,
        Telemetry $telemetry,
        private UrlSanitizer $urlSanitizer = new UrlSanitizer(),
    ) {
        $this->clientUrlAttributes = new ClientUrlAttributes($urlSanitizer);
        $this->tracer = $telemetry->tracer(
            'flow.psr18.http_client',
            PackageVersion::get('flow-php/psr18-telemetry-bridge'),
        );
    }

    public function sendRequest(RequestInterface $request): ResponseInterface
    {
        $method = $request->getMethod();
        $url = (string) $request->getUri();
        $attributes = [SemConvAttributes::HTTP_REQUEST_METHOD => $method] + $this->clientUrlAttributes->of($url);

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
            $message = $this->urlSanitizer->message($exception->getMessage(), $url);

            $span->recordException($exception, new DateTimeImmutable(), ['exception.message' => $message]);
            $span->setAttribute(SemConvAttributes::ERROR_TYPE, $exception::class);
            $span->setStatus(SpanStatus::error($message));

            throw $exception;
        } finally {
            $scope->detach();
            $this->tracer->complete($span);
        }
    }
}
