<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Generator;

final class RecordingSourceStream implements SourceStream
{
    public function __construct(
        private readonly SourceStream $stream,
        private readonly RecordingFilesystem $filesystem,
    ) {}

    public function close(): void
    {
        $this->filesystem->record('closeSource');

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

    public function size(): ?int
    {
        return $this->stream->size();
    }
}
