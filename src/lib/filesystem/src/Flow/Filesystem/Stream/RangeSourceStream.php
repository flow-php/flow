<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Generator;

use function array_pop;
use function explode;
use function max;
use function min;
use function str_contains;
use function strlen;

/**
 * Reads go through the inner read(), so a remote stream fetches only the window.
 */
final readonly class RangeSourceStream implements SourceStream
{
    private const int CHUNK = 1 << 15;

    private int $end;

    /**
     * @var int<0, max>
     */
    private int $start;

    /**
     * @param int $start first byte of the window
     * @param int $end first byte past the window
     */
    public function __construct(
        private SourceStream $stream,
        int $start,
        int $end,
    ) {
        if ($start < 0) {
            throw new InvalidArgumentException('Range start must not be negative, given: ' . $start);
        }

        if ($end < $start) {
            throw new InvalidArgumentException('Range end must not precede its start, given: ' . $start . '..' . $end);
        }

        $this->start = $start;
        $this->end = $end;
    }

    public function close(): void
    {
        $this->stream->close();
    }

    public function content(): string
    {
        $length = $this->end - $this->start;

        return $length < 1 ? '' : $this->stream->read($length, $this->start);
    }

    public function isOpen(): bool
    {
        return $this->stream->isOpen();
    }

    public function iterate(int $length = 1): Generator
    {
        $position = $this->start;

        while (($remaining = min($length, $this->end - $position)) > 0) {
            $chunk = $this->stream->read($remaining, $position);

            if ($chunk === '') {
                return;
            }

            $position += strlen($chunk);

            yield $chunk;
        }
    }

    public function path(): Path
    {
        return $this->stream->path();
    }

    /**
     * @param int $offset relative to the window's start; negative counts back from the window's end
     */
    public function read(int $length, int $offset): string
    {
        $from = $offset < 0 ? max($this->start, $this->end + $offset) : $this->start + $offset;
        $available = $this->end - $from;

        return $available < 1 ? '' : $this->stream->read(min($length, $available), $from);
    }

    public function readLines(string $separator = "\n", ?int $length = null): Generator
    {
        $content = '';

        foreach ($this->iterate($length ?? self::CHUNK) as $chunk) {
            $content .= $chunk;

            if (!str_contains($content, $separator)) {
                continue;
            }

            $lines = explode($separator, $content);
            $content = array_pop($lines);

            foreach ($lines as $line) {
                yield $line;
            }
        }

        if ($content !== '') {
            yield $content;
        }
    }

    public function size(): int
    {
        return $this->end - $this->start;
    }

    /**
     * First byte of the window in the inner stream.
     *
     * @return int<0, max>
     */
    public function start(): int
    {
        return $this->start;
    }

    /**
     * First byte past the window in the inner stream: start() + size().
     */
    public function end(): int
    {
        return $this->end;
    }

    /**
     * The stream the window is cut from.
     */
    public function stream(): SourceStream
    {
        return $this->stream;
    }
}
