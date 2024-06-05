<?php

declare(strict_types=1);

namespace Flow\Parquet;

interface Stream
{
    public function close() : void;

    public function isOpen() : bool;

    public function read(int $length, int $offset, int $whence) : string;
}
