<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream;

use Flow\Filesystem\{DestinationStream, Path, SourceStream};

final class VoidStream implements DestinationStream, SourceStream
{
    public function __construct(private readonly Path $path)
    {

    }

    public function append(string $data) : self
    {
        return $this;
    }

    public function close() : void
    {
    }

    public function content() : string
    {
        return '';
    }

    public function fromResource($resource) : self
    {
        return $this;
    }

    public function isOpen() : bool
    {
        return true;
    }

    public function iterate(int $length = 1) : \Generator
    {
        /** @phpstan-ignore-next-line */
        foreach ([] as $char) {
            yield $char;
        }
    }

    public function path() : Path
    {
        return $this->path;
    }

    public function read(int $length, int $offset) : string
    {
        return '';
    }

    public function readLines(string $separator = "\n", ?int $length = null) : \Generator
    {
        /** @phpstan-ignore-next-line */
        foreach ([] as $char) {
            yield $char;
        }
    }

    public function size() : int
    {
        return 0;
    }
}
