<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\HttpClient;

use Symfony\Contracts\HttpClient\ChunkInterface;

final readonly class Chunk implements ChunkInterface
{
    public function __construct(
        private bool $isFirst = false,
        private bool $isLast = false,
        private bool $isTimeout = false,
        private ?string $error = null,
        private string $content = '',
        private int $offset = 0,
    ) {}

    public function getContent(): string
    {
        return $this->content;
    }

    public function getError(): ?string
    {
        return $this->error;
    }

    public function getInformationalStatus(): ?array
    {
        return null;
    }

    public function getOffset(): int
    {
        return $this->offset;
    }

    public function isFirst(): bool
    {
        return $this->isFirst;
    }

    public function isLast(): bool
    {
        return $this->isLast;
    }

    public function isTimeout(): bool
    {
        return $this->isTimeout;
    }
}
