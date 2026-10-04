<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Double;

use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Generator;

use function strlen;

final class ClosingSpySourceStream implements SourceStream
{
    public int $closeCount = 0;

    public int $readBytes = 0;

    public function __construct(
        private readonly SourceStream $stream,
    ) {}

    public function close(): void
    {
        $this->closeCount++;
        $this->stream->close();
    }

    public function content(): string
    {
        $content = $this->stream->content();
        $this->readBytes += strlen($content);

        return $content;
    }

    public function isOpen(): bool
    {
        return $this->stream->isOpen();
    }

    public function iterate(int $length = 1): Generator
    {
        foreach ($this->stream->iterate($length) as $chunk) {
            $this->readBytes += strlen($chunk);

            yield $chunk;
        }
    }

    public function path(): Path
    {
        return $this->stream->path();
    }

    public function read(int $length, int $offset): string
    {
        $bytes = $this->stream->read($length, $offset);
        $this->readBytes += strlen($bytes);

        return $bytes;
    }

    public function readLines(string $separator = "\n", ?int $length = null): Generator
    {
        return $this->stream->readLines($separator, $length);
    }

    public function size(): ?int
    {
        return $this->stream->size();
    }
}
