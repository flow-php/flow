<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\GetBlobProperties;

use Psr\Http\Message\ResponseInterface;

final readonly class BlobProperties
{
    public function __construct(
        private ResponseInterface $response,
    ) {}

    public function content(): string
    {
        return (string) $this->response->getBody();
    }

    public function lastModifiedAt(): ?\DateTimeImmutable
    {
        $raw = $this->response->getHeaderLine('Last-Modified');

        if ($raw === '') {
            return null;
        }

        $parsed = \DateTimeImmutable::createFromFormat(\DateTimeImmutable::RFC7231, $raw);

        return $parsed === false ? null : $parsed;
    }

    public function size(): int
    {
        return (int) $this->response->getHeaderLine('Content-Length');
    }
}
