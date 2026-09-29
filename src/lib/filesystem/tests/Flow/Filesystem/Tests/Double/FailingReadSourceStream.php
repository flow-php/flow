<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Double;

use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Generator;

use function sprintf;

final readonly class FailingReadSourceStream implements SourceStream
{
    public function __construct(
        private SourceStream $inner,
    ) {}

    public function close(): void
    {
        $this->inner->close();
    }

    public function content(): string
    {
        return $this->inner->content();
    }

    public function isOpen(): bool
    {
        return $this->inner->isOpen();
    }

    public function iterate(int $length = 1): Generator
    {
        return $this->inner->iterate($length);
    }

    public function path(): Path
    {
        return $this->inner->path();
    }

    public function read(int $length, int $offset): string
    {
        throw new RuntimeException(sprintf('Reading "%s" failed', $this->inner->path()->uri()));
    }

    public function readLines(string $separator = "\n", ?int $length = null): Generator
    {
        return $this->inner->readLines($separator, $length);
    }

    public function size(): ?int
    {
        return $this->inner->size();
    }
}
