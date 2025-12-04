<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream;

use Flow\Filesystem\{DestinationStream, Path, SourceStream};

final readonly class VoidStream implements DestinationStream, SourceStream
{
    public function __construct(private Path $path)
    {

    }

    #[\Override]
    public function append(string $data) : self
    {
        return $this;
    }

    #[\Override]
    public function close() : void
    {
    }

    #[\Override]
    public function content() : string
    {
        return '';
    }

    #[\Override]
    public function fromResource($resource) : self
    {
        return $this;
    }

    #[\Override]
    public function isOpen() : bool
    {
        return true;
    }

    #[\Override]
    public function iterate(int $length = 1) : \Generator
    {
        /** @phpstan-ignore-next-line */
        foreach ([] as $char) {
            yield $char;
        }
    }

    #[\Override]
    public function path() : Path
    {
        return $this->path;
    }

    #[\Override]
    public function read(int $length, int $offset) : string
    {
        return '';
    }

    #[\Override]
    public function readLines(string $separator = "\n", ?int $length = null) : \Generator
    {
        /** @phpstan-ignore-next-line */
        foreach ([] as $char) {
            yield $char;
        }
    }

    #[\Override]
    public function size() : int
    {
        return 0;
    }
}
