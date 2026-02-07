<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\HttpClient;

use Symfony\Contracts\HttpClient\{HttpClientInterface, ResponseInterface, ResponseStreamInterface};

final readonly class SuccessHttpClient implements HttpClientInterface
{
    public function __construct(
        private int $statusCode = 200,
    ) {
    }

    /**
     * @param array<string, mixed> $options
     */
    public function request(string $method, string $url, array $options = []) : ResponseInterface
    {
        return new MockResponse($this->statusCode);
    }

    public function stream(ResponseInterface|iterable $responses, ?float $timeout = null) : ResponseStreamInterface
    {
        throw new \RuntimeException('stream() not implemented in test fixture');
    }

    /**
     * @param array<string, mixed> $options
     */
    public function withOptions(array $options) : static
    {
        return $this;
    }
}
