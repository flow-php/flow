<?php

declare(strict_types=1);

namespace Flow\Filesystem;

use Generator;

interface SourceStream extends Stream
{
    public function content(): string;

    /**
     * @param int<1, max> $length number of bytes to read from the stream
     *
     * @return \Generator<string>
     */
    public function iterate(int $length = 1): Generator;

    /**
     * @param int<1, max> $length number of bytes to read from the stream
     * @param int $offset The offset where to start reading from the stream. If negative, reading will start from the end of the stream.
     */
    public function read(int $length, int $offset): string;

    /**
     * Yields the stream's lines with the separator stripped: a 0-byte stream yields nothing, a separator that ends the
     * stream opens no further line, and every other empty line is yielded as ''. The separator is matched byte for
     * byte; no "\r" is stripped. "x\ny\n" -> ["x","y"], "x\n\ny" -> ["x","","y"], "x\n\n" -> ["x",""],
     * "x\r\ny" -> ["x\r","y"], "" -> [].
     *
     * @param non-empty-string $separator multi-byte included
     * @param null|int<1, max> $length bytes read per step - a read-chunk hint, never a cap on line length; null lets the implementation choose
     *
     * @return \Generator<string>
     */
    public function readLines(string $separator = "\n", ?int $length = null): Generator;

    /**
     * @return null|int The size of the stream in bytes
     */
    public function size(): ?int;
}
