<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Double;

use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Generator;

use function strlen;

/**
 * Counts the read() calls made through it and the bytes they returned.
 */
final class ReadCountingSourceStream implements SourceStream
{
    public int $bytes = 0;

    public bool $closed = false;

    public int $reads = 0;

    public function __construct(
        private readonly SourceStream $inner,
    ) {}

    public function close(): void
    {
        $this->closed = true;
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
        yield from $this->inner->iterate($length);
    }

    public function path(): Path
    {
        return $this->inner->path();
    }

    public function read(int $length, int $offset): string
    {
        $read = $this->inner->read($length, $offset);
        $this->reads++;
        $this->bytes += strlen($read);

        return $read;
    }

    public function readLines(string $separator = "\n", ?int $length = null): Generator
    {
        yield from $this->inner->readLines($separator, $length);
    }

    public function size(): ?int
    {
        return $this->inner->size();
    }
}
