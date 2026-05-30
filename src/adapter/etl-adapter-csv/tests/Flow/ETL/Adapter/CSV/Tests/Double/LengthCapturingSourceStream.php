<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Double;

use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Generator;

use function explode;
use function strlen;
use function substr;

final class LengthCapturingSourceStream implements SourceStream
{
    /**
     * @var array<null|int> the length argument captured on each readLines() call
     */
    public array $capturedLengths = [];

    public function __construct(
        private readonly string $contents,
        private readonly Path $path,
    ) {}

    public function close(): void {}

    public function content(): string
    {
        return $this->contents;
    }

    public function isOpen(): bool
    {
        return true;
    }

    public function iterate(int $length = 1): Generator
    {
        for ($i = 0; $i < strlen($this->contents); $i += $length) {
            yield substr($this->contents, $i, $length);
        }
    }

    public function path(): Path
    {
        return $this->path;
    }

    public function read(int $length, int $offset): string
    {
        return substr($this->contents, $offset, $length);
    }

    public function readLines(string $separator = "\n", ?int $length = null): Generator
    {
        $this->capturedLengths[] = $length;

        foreach (explode($separator, $this->contents) as $line) {
            yield $line;
        }
    }

    public function size(): int
    {
        return strlen($this->contents);
    }
}
