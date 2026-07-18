<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Path;

use function stream_get_contents;

final class StringDestinationStream implements DestinationStream
{
    private string $buffer = '';

    public function __construct(
        private readonly Path $path,
    ) {}

    public function append(string $data): DestinationStream
    {
        $this->buffer .= $data;

        return $this;
    }

    public function content(): string
    {
        return $this->buffer;
    }

    public function fromResource($resource): DestinationStream
    {
        $this->buffer .= (string) stream_get_contents($resource);

        return $this;
    }

    public function close(): void {}

    public function isOpen(): bool
    {
        return true;
    }

    public function path(): Path
    {
        return $this->path;
    }
}
