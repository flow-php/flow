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

    #[\Override]
    public function close() : void
    {
    }

    #[\Override]
    public function content() : string
    {
        return $this->content;
    }

    #[\Override]
    public function isOpen() : bool
    {
        return true;
    }

    #[\Override]
    public function iterate(int $length = 1) : \Generator
    {
        foreach (\str_split($this->content, $length) as $chunk) {
            yield $chunk;
        }
    }

    #[\Override]
    public function path() : Path
    {
        return \Flow\Filesystem\DSL\path('memory://');
    }

    #[\Override]
    public function read(int $length, int $offset) : string
    {
        return \substr($this->content, $offset, $length);
    }

    #[\Override]
    public function readLines(string $separator = "\n", ?int $length = null) : \Generator
    {
        /** @phpstan-ignore-next-line */
        foreach (\explode($separator, $this->content) as $line) {
            if (\strlen($line)) {
                yield $line;
            }
        }
    }

    #[\Override]
    public function size() : int
    {
        return \strlen($this->content);
    }
}
