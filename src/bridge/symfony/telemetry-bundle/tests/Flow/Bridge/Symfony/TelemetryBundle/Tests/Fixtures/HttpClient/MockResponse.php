<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\HttpClient;

use Symfony\Contracts\HttpClient\ResponseInterface;

final readonly class MockResponse implements ResponseInterface
{
    public function __construct(
        private int $statusCode = 200,
    ) {}

    public function cancel(): void {}

    public function getContent(bool $throw = true): string
    {
        return '';
    }

    /**
     * @return array<string, list<string>>
     */
    public function getHeaders(bool $throw = true): array
    {
        return [];
    }

    public function getInfo(?string $type = null): mixed
    {
        if ($type === 'http_code') {
            return $this->statusCode;
        }

        if ($type === null) {
            return ['http_code' => $this->statusCode];
        }

        return null;
    }

    public function getStatusCode(): int
    {
        return $this->statusCode;
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray(bool $throw = true): array
    {
        return [];
    }
}
