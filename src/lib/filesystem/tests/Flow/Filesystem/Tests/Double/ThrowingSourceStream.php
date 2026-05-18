<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Double;

use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Generator;

final class ThrowingSourceStream implements SourceStream
{
    public bool $closed = false;

    public function __construct(
        private readonly Path $path,
    ) {}

    public function close(): void
    {
        $this->closed = true;
    }

    public function content(): string
    {
        throw new RuntimeException('Throwing source stream failed during content()');
    }

    public function isOpen(): bool
    {
        return !$this->closed;
    }

    public function iterate(int $length = 1): Generator
    {
        throw new RuntimeException('Throwing source stream failed mid-iterate');

        /** @mago-ignore analysis:unevaluated-code */
        yield;
    }

    public function path(): Path
    {
        return $this->path;
    }

    public function read(int $length, int $offset): string
    {
        throw new RuntimeException('Throwing source stream failed during read()');
    }

    public function readLines(string $separator = "\n", ?int $length = null): Generator
    {
        throw new RuntimeException('Throwing source stream failed during readLines()');

        /** @mago-ignore analysis:unevaluated-code */
        yield;
    }

    public function size(): ?int
    {
        return null;
    }
}
