<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream\Block;

use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Stream\{NativeLocalDestinationStream, NativeLocalSourceStream};

final class NativeLocalStreamBlock
{
    private int $currentSize = 0;

    public function __construct(private readonly string $id, private readonly int $size, private readonly NativeLocalDestinationStream $stream)
    {
        if (!$stream->path()->protocol()->is('file')) {
            throw new RuntimeException('FileBlock can be used only with file:// protocol, got: ' . $stream->path()->protocol()->scheme());
        }
    }

    public function append(string $data) : void
    {
        if ($this->spaceLeft() < strlen($data)) {
            throw new RuntimeException('Block is full, space left: ' . $this->spaceLeft() . ' bytes, trying to append: ' . strlen($data) . ' bytes.');
        }

        $this->currentSize += strlen($data);
    }

    public function id() : string
    {
        return $this->id;
    }

    public function read() : NativeLocalSourceStream
    {
        return NativeLocalSourceStream::open($this->stream->path());
    }

    public function size() : int
    {
        return $this->currentSize;
    }

    public function spaceLeft() : int
    {
        return $this->size - $this->currentSize;
    }
}
