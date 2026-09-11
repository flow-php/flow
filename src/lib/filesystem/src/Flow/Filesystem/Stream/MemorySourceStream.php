<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Generator;

use function Flow\Filesystem\DSL\path;
use function strlen;

final readonly class MemorySourceStream implements SourceStream
{
    private StringSourceStream $stream;

    /**
     * @param non-empty-string $content
     *
     * @throws InvalidArgumentException
     */
    public function __construct(string $content)
    {
        if (!strlen($content)) {
            throw new InvalidArgumentException('MemorySourceStream expects non-empty content');
        }

        $this->stream = new StringSourceStream(path('memory://'), $content);
    }

    public function close(): void
    {
        $this->stream->close();
    }

    public function content(): string
    {
        return $this->stream->content();
    }

    public function isOpen(): bool
    {
        return $this->stream->isOpen();
    }

    public function iterate(int $length = 1): Generator
    {
        return $this->stream->iterate($length);
    }

    public function path(): Path
    {
        return $this->stream->path();
    }

    public function read(int $length, int $offset): string
    {
        return $this->stream->read($length, $offset);
    }

    public function readLines(string $separator = "\n", ?int $length = null): Generator
    {
        return $this->stream->readLines($separator, $length);
    }

    public function size(): int
    {
        return $this->stream->size();
    }
}
