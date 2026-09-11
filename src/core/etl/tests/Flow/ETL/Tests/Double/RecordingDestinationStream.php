<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Path;

final class RecordingDestinationStream implements DestinationStream
{
    public function __construct(
        private readonly DestinationStream $stream,
        private readonly RecordingFilesystem $filesystem,
    ) {}

    public function append(string $data): self
    {
        $this->filesystem->record('append');

        $this->stream->append($data);

        return $this;
    }

    public function close(): void
    {
        $this->filesystem->record('close');

        $this->stream->close();
    }

    public function fromResource($resource): self
    {
        $this->stream->fromResource($resource);

        return $this;
    }

    public function isOpen(): bool
    {
        return $this->stream->isOpen();
    }

    public function path(): Path
    {
        return $this->stream->path();
    }
}
