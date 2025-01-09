<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream;

use Flow\Filesystem\{Exception\InvalidArgumentException, Path, SourceStream};

final readonly class MemorySourceStream implements SourceStream
{
    /**
     * @param non-empty-string $content
     *
     * @throws InvalidArgumentException
     */
    public function __construct(private string $content)
    {
        if (!\strlen($this->content)) {
            throw new InvalidArgumentException('MemorySourceStream expects non-empty content');
        }
    }

    public function close() : void
    {
    }

    public function content() : string
    {
        return $this->content;
    }

    public function isOpen() : bool
    {
        return true;
    }

    public function iterate(int $length = 1) : \Generator
    {
        foreach (\str_split($this->content, $length) as $chunk) {
            yield $chunk;
        }
    }

    public function path() : Path
    {
        return new Path('memory://');
    }

    public function read(int $length, int $offset) : string
    {
        return \substr($this->content, $offset, $length);
    }

    public function readLines(string $separator = "\n", ?int $length = null) : \Generator
    {
        /** @phpstan-ignore-next-line */
        foreach (\explode($separator, $this->content) as $line) {
            if (\strlen($line)) {
                yield $line;
            }
        }
    }

    public function size() : int
    {
        return \strlen($this->content);
    }
}
