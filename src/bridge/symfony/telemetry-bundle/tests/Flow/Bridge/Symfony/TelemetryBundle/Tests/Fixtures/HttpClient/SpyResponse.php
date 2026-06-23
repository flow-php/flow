<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\HttpClient;

use Symfony\Contracts\HttpClient\ResponseInterface;
use Throwable;

final class SpyResponse implements ResponseInterface
{
    public bool $cancelCalled = false;

    public bool $destructCalled = false;

    public bool $getContentCalled = false;

    public bool $getHeadersCalled = false;

    public bool $getInfoCalled = false;

    public bool $getStatusCodeCalled = false;

    public bool $toArrayCalled = false;

    public function __construct(
        private readonly int $statusCode = 200,
        private readonly ?Throwable $throwOnAccess = null,
    ) {}

    public function __destruct()
    {
        $this->destructCalled = true;
    }

    public function anyMethodCalled(): bool
    {
        return (
            $this->getStatusCodeCalled
            || $this->getHeadersCalled
            || $this->getContentCalled
            || $this->toArrayCalled
            || $this->cancelCalled
            || $this->getInfoCalled
        );
    }

    public function cancel(): void
    {
        $this->cancelCalled = true;
    }

    public function getContent(bool $throw = true): string
    {
        $this->getContentCalled = true;

        if ($this->throwOnAccess !== null) {
            throw $this->throwOnAccess;
        }

        return '';
    }

    /**
     * @return array<string, list<string>>
     */
    public function getHeaders(bool $throw = true): array
    {
        $this->getHeadersCalled = true;

        if ($this->throwOnAccess !== null) {
            throw $this->throwOnAccess;
        }

        return [];
    }

    public function getInfo(?string $type = null): mixed
    {
        $this->getInfoCalled = true;

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
        $this->getStatusCodeCalled = true;

        if ($this->throwOnAccess !== null) {
            throw $this->throwOnAccess;
        }

        return $this->statusCode;
    }

    /**
     * @return array<array-key, mixed>
     */
    public function toArray(bool $throw = true): array
    {
        $this->toArrayCalled = true;

        if ($this->throwOnAccess !== null) {
            throw $this->throwOnAccess;
        }

        return [];
    }
}
