<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Generator;

use function array_key_exists;
use function array_key_first;
use function count;
use function intdiv;
use function min;
use function strlen;
use function substr;

/**
 * Serves read() from the last two fixed-size blocks it fetched, so many small reads that walk a region forward - a
 * scan probing candidate positions - cost one inner read per block instead of one per call.
 */
final class BlockCachedSourceStream implements SourceStream
{
    /**
     * @var array<int, string> block start => block bytes
     */
    private array $blocks = [];

    /**
     * @var int<1, max>
     */
    private readonly int $blockSize;

    public function __construct(
        private readonly SourceStream $stream,
        int $blockSize = 1 << 16,
    ) {
        if ($blockSize < 1) {
            throw new InvalidArgumentException('Block size must be greater than 0, given: ' . $blockSize);
        }

        $this->blockSize = $blockSize;
    }

    public function close(): void
    {
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

    /**
     * @param int $offset a negative offset counts from the end and bypasses the cache
     */
    public function read(int $length, int $offset): string
    {
        if ($offset < 0) {
            return $this->stream->read($length, $offset);
        }

        $bytes = '';

        while ($length > 0) {
            $start = intdiv($offset, $this->blockSize) * $this->blockSize;

            if (!array_key_exists($start, $this->blocks)) {
                if (count($this->blocks) >= 2) {
                    unset($this->blocks[array_key_first($this->blocks)]);
                }

                $this->blocks[$start] = $this->stream->read($this->blockSize, $start);
            }

            $chunk = substr($this->blocks[$start], $offset - $start, min($length, $this->blockSize));

            if ($chunk === '') {
                break;
            }

            $bytes .= $chunk;
            $offset += strlen($chunk);
            $length -= strlen($chunk);
        }

        return $bytes;
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
