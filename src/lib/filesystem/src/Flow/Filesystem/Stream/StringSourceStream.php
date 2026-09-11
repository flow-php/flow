<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream;

use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Generator;

use function array_pop;
use function end;
use function explode;
use function str_split;
use function strlen;
use function substr;

final readonly class StringSourceStream implements SourceStream
{
    public function __construct(
        private Path $path,
        private string $content,
    ) {}

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
        return $this->path;
    }

    public function read(int $length, int $offset): string
    {
        return substr($this->content, $offset, $length);
    }

    public function readLines(string $separator = "\n", ?int $length = null): Generator
    {
        $lines = explode($separator, $this->content);

        if (end($lines) === '') {
            array_pop($lines);
        }

        foreach ($lines as $line) {
            yield $line;
        }
    }

    public function size(): int
    {
        return strlen($this->content);
    }
}
