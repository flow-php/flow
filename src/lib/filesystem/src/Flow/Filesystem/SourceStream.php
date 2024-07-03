<?php

declare(strict_types=1);

namespace Flow\Filesystem;

interface SourceStream extends Stream
{
    public function content() : string;

    /**
     * @param int<1, max> $length number of bytes to read from the stream
     *
     * @return \Generator<string>
     */
    public function iterate(int $length = 1) : \Generator;

    /**
     * @param int<1, max> $length number of bytes to read from the stream
     * @param int $offset The offset where to start reading from the stream. If negative, reading will start from the end of the stream.
     */
    public function read(int $length, int $offset) : string;

    /**
     * @param string $separator The line separator, content will be read until the first occurrence of the separator
     * @param null|int<1, max> $length Number of bytes to read in one step. If the end of the stream or separator is reached before the specified number of bytes are read, the remaining bytes are returned.
     *                                 Otherwise we are reading until the separator is found or end of file is reached. When working with remote streams it might be a good idea to set length to few mb in orders to reduce number of network requests.
     *                                 When no value is provided, filesystems will use a default value, for example NativeLocalFilesystem is going to use 8192 length.
     *
     * @return \Generator<string>
     */
    public function readLines(string $separator = "\n", ?int $length = null) : \Generator;

    /**
     * @return null|int The size of the stream in bytes
     */
    public function size() : ?int;
}
