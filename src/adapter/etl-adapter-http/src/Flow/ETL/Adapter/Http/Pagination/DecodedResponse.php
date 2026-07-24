<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Pagination;

use Psr\Http\Message\ResponseInterface;

final class DecodedResponse
{
    /**
     * @param array<mixed> $body
     */
    public function __construct(
        private readonly array $body,
        private readonly ResponseInterface $response,
    ) {}

    /**
     * @return array<mixed>
     */
    public function body(): array
    {
        return $this->body;
    }

    /**
     * @return array<string>
     */
    public function header(string $name): array
    {
        return $this->response->getHeader($name);
    }

    public function response(): ResponseInterface
    {
        return $this->response;
    }

    public function statusCode(): int
    {
        return $this->response->getStatusCode();
    }
}
