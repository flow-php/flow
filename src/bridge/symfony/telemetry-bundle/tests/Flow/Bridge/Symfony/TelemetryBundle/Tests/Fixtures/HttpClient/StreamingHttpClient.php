<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\HttpClient;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpClient\ResponseStream;
use Generator;
use Symfony\Contracts\HttpClient\HttpClientInterface;
use Symfony\Contracts\HttpClient\ResponseInterface;
use Symfony\Contracts\HttpClient\ResponseStreamInterface;

final readonly class StreamingHttpClient implements HttpClientInterface
{
    public function __construct(
        private int $statusCode = 200,
        private ?string $error = null,
    ) {}

    /**
     * @param array<array-key, mixed> $options
     */
    public function request(string $method, string $url, array $options = []): ResponseInterface
    {
        return new MockResponse($this->statusCode);
    }

    public function stream(ResponseInterface|iterable $responses, ?float $timeout = null): ResponseStreamInterface
    {
        if ($responses instanceof ResponseInterface) {
            $responses = [$responses];
        }

        return new ResponseStream($this->generate($responses));
    }

    /**
     * @param array<array-key, mixed> $options
     */
    public function withOptions(array $options): static
    {
        return $this;
    }

    /**
     * @param iterable<ResponseInterface> $responses
     *
     * @return Generator<ResponseInterface, Chunk>
     */
    private function generate(iterable $responses): Generator
    {
        foreach ($responses as $response) {
            if ($this->error !== null) {
                yield $response => new Chunk(error: $this->error);

                continue;
            }

            yield $response => new Chunk(isFirst: true);
            yield $response => new Chunk(isLast: true);
        }
    }
}
