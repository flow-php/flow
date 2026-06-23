<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpClient;

use DateTimeImmutable;
use Flow\Bridge\Symfony\TelemetryBundle\Exception\RuntimeException;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\Tracer;
use Generator;
use SplObjectStorage;
use Symfony\Contracts\HttpClient\ChunkInterface;
use Symfony\Contracts\HttpClient\HttpClientInterface;
use Symfony\Contracts\HttpClient\ResponseInterface;
use Throwable;
use TypeError;

use function get_debug_type;
use function is_int;
use function method_exists;
use function sprintf;

/**
 * Lazy response decorator that finalizes a CLIENT span when the response is actually consumed.
 */
final class TraceableResponse implements ResponseInterface
{
    private bool $completed = false;

    private bool $statusRecorded = false;

    public function __construct(
        private readonly Tracer $tracer,
        private readonly ResponseInterface $response,
        private readonly Span $span,
    ) {}

    public function __destruct()
    {
        try {
            if (method_exists($this->response, '__destruct')) {
                $this->response->__destruct();
            }
        } finally {
            $this->recordStatusFromInfo();
            $this->complete();
        }
    }

    public function cancel(): void
    {
        $this->response->cancel();
        $this->recordStatusFromInfo();
        $this->complete();
    }

    public function getContent(bool $throw = true): string
    {
        try {
            $content = $this->response->getContent($throw);
            $this->recordStatus($this->response->getStatusCode());

            return $content;
        } catch (Throwable $exception) {
            $this->failAndComplete($exception);

            throw $exception;
        } finally {
            $this->complete();
        }
    }

    /**
     * @return array<string, list<string>>
     */
    public function getHeaders(bool $throw = true): array
    {
        try {
            $headers = $this->response->getHeaders($throw);
            $this->recordStatus($this->response->getStatusCode());

            return $headers;
        } catch (Throwable $exception) {
            $this->failAndComplete($exception);

            throw $exception;
        }
    }

    public function getInfo(?string $type = null): mixed
    {
        return $this->response->getInfo($type);
    }

    public function getStatusCode(): int
    {
        try {
            $statusCode = $this->response->getStatusCode();
            $this->recordStatus($statusCode);

            return $statusCode;
        } catch (Throwable $exception) {
            $this->failAndComplete($exception);

            throw $exception;
        }
    }

    /**
     * @return array<array-key, mixed>
     */
    public function toArray(bool $throw = true): array
    {
        try {
            $data = $this->response->toArray($throw);
            $this->recordStatus($this->response->getStatusCode());

            return $data;
        } catch (Throwable $exception) {
            $this->failAndComplete($exception);

            throw $exception;
        } finally {
            $this->complete();
        }
    }

    /**
     * Drives a multiplexed stream over wrapped responses, finalizing each span when its stream ends.
     *
     * @param iterable<array-key, ResponseInterface> $responses
     *
     * @return Generator<TraceableResponse, ChunkInterface>
     */
    public static function stream(HttpClientInterface $client, iterable $responses, ?float $timeout): Generator
    {
        $wrappedResponses = [];
        /** @var SplObjectStorage<ResponseInterface, self> $traceableMap */
        $traceableMap = new SplObjectStorage();

        foreach ($responses as $response) {
            if (!$response instanceof self) {
                throw new TypeError(sprintf(
                    '"%s::stream()" expects parameter 1 to be an iterable of TracableResponse objects, "%s" given.',
                    TracableHttpClient::class,
                    get_debug_type($response),
                ));
            }

            $traceableMap[$response->response] = $response;
            $wrappedResponses[] = $response->response;
        }

        foreach ($client->stream($wrappedResponses, $timeout) as $response => $chunk) {
            $wrapper = $traceableMap[$response];

            $error = $chunk->getError();

            if ($error !== null) {
                $wrapper->failAndComplete(new RuntimeException($error));
            } elseif ($chunk->isFirst()) {
                $wrapper->recordStatus($response->getStatusCode());
            } elseif ($chunk->isLast()) {
                $wrapper->recordStatus($response->getStatusCode());
                $wrapper->complete();
            }

            yield $wrapper => $chunk;
        }
    }

    private function complete(): void
    {
        if ($this->completed) {
            return;
        }

        $this->completed = true;
        $this->tracer->complete($this->span);
    }

    private function failAndComplete(Throwable $exception): void
    {
        $this->span->recordException($exception, new DateTimeImmutable());
        $this->span->setStatus(SpanStatus::error($exception->getMessage()));
        $this->complete();
    }

    private function recordStatus(int $statusCode): void
    {
        if ($this->statusRecorded) {
            return;
        }

        $this->statusRecorded = true;
        $this->span->setAttribute('http.response.status_code', $statusCode);

        if ($statusCode >= 400) {
            $this->span->setStatus(SpanStatus::error("HTTP {$statusCode}"));
        } else {
            $this->span->setStatus(SpanStatus::ok());
        }
    }

    private function recordStatusFromInfo(): void
    {
        $this->recordResolvedStatus($this->response->getInfo('http_code'));
    }

    private function recordResolvedStatus(mixed $statusCode): void
    {
        if (is_int($statusCode) && $statusCode > 0) {
            $this->recordStatus($statusCode);
        }
    }
}
