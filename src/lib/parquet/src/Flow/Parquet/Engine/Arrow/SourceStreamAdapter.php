<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine\Arrow;

use Flow\Arrow\RandomAccessFile;
use Flow\Filesystem\SourceStream;

final readonly class SourceStreamAdapter implements RandomAccessFile
{
    public function __construct(private SourceStream $stream)
    {
    }

    public function read(int $length, int $offset) : string
    {
        /** @var int<1, max> $length */
        return $this->stream->read($length, $offset);
    }

    public function size() : ?int
    {
        return $this->stream->size();
    }
}
