<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\HttpClient;

use Symfony\Contracts\HttpClient\{HttpClientInterface, ResponseInterface, ResponseStreamInterface};

final readonly class FailingHttpClient implements HttpClientInterface
{
    public function __construct(
        private string $message = 'Connection failed',
    ) {
    }

    /**
     * @param array<string, mixed> $options
     */
    public function request(string $method, string $url, array $options = []) : ResponseInterface
    {
        throw new \RuntimeException($this->message);
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
