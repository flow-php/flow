<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpClient;

use DateTimeImmutable;
use Flow\Telemetry\ClientUrlAttributes;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Sanitizer\UrlSanitizer;
use Flow\Telemetry\SemConvAttributes;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Symfony\Contracts\HttpClient\HttpClientInterface;
use Symfony\Contracts\HttpClient\ResponseInterface;
use Symfony\Contracts\HttpClient\ResponseStreamInterface;
use Throwable;

use function is_string;

final readonly class TracableHttpClient implements HttpClientInterface
{
    private ClientUrlAttributes $clientUrlAttributes;

    public function __construct(
        private HttpClientInterface $client,
        private Telemetry $telemetry,
        private string $clientName,
        private UrlSanitizer $urlSanitizer = new UrlSanitizer(),
    ) {
        $this->clientUrlAttributes = new ClientUrlAttributes($urlSanitizer);
    }

    /**
     * @param array<array-key, mixed> $options
     */
    public function request(string $method, string $url, array $options = []): ResponseInterface
    {
        $tracer = $this->telemetry->tracer('flow.symfony.http_client', PackageVersion::get('symfony/http-client'));

        $attributes =
            [
                SemConvAttributes::HTTP_REQUEST_METHOD => $method,
                HttpClientAttributes::ATTR_CLIENT_NAME => $this->clientName,
            ] + $this->clientUrlAttributes->of($url);

        // OTEL HTTP semconv: client span name is "{method}" - host would be per-host cardinality.
        $span = $tracer->span($method, SpanKind::CLIENT, $attributes);
        $scope = $tracer->activate($span);

        try {
            $response = $this->client->request($method, $url, $options);
        } catch (Throwable $exception) {
            $message = $this->urlSanitizer->message($exception->getMessage(), $url);

            $span->recordException($exception, new DateTimeImmutable(), ['exception.message' => $message]);
            $span->setStatus(SpanStatus::error($message));
            $scope->detach();
            $tracer->complete($span);

            throw $exception;
        }

        // base_uri and the query option make the effective URL differ from the $url argument.
        // @mago-expect analysis:mixed-assignment
        $effectiveUrl = $response->getInfo('url');

        if (is_string($effectiveUrl) && $effectiveUrl !== '') {
            $span->setAttributes($this->clientUrlAttributes->of($effectiveUrl));
        }

        return new TraceableResponse($tracer, $response, $span, $this->urlSanitizer);
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
        return new self($this->client->withOptions($options), $this->telemetry, $this->clientName, $this->urlSanitizer);
    }
}
