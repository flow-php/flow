<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Generator;

use function explode;
use function Flow\Filesystem\DSL\path;
use function str_split;
use function strlen;
use function substr;

final readonly class MemorySourceStream implements SourceStream
{
    /**
     * @param non-empty-string $content
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private string $content,
    ) {
        if (!strlen($this->content)) {
            throw new InvalidArgumentException('MemorySourceStream expects non-empty content');
        }
    }

    public function close(): void {}

    public function content(): string
    {
        return $this->content;
    }

    public function isOpen(): bool
    {
        return true;
    }

    public function iterate(int $length = 1): Generator
    {
        foreach (str_split($this->content, $length) as $chunk) {
            yield $chunk;
        }
    }

    public function path(): Path
    {
        return path('memory://');
    }

    public function read(int $length, int $offset): string
    {
        return substr($this->content, $offset, $length);
    }

    public function readLines(string $separator = "\n", ?int $length = null): Generator
    {
        foreach (explode($separator, $this->content) as $line) {
            if (strlen($line)) {
                yield $line;
            }
        }
    }

    public function size(): int
    {
        return strlen($this->content);
    }
}
