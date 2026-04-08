<?php

declare(strict_types=1);

namespace Flow\Arrow;

if (\extension_loaded('arrow')) {
    return;
}

interface RandomAccessFile
{
    public function read(int $length, int $offset) : string;

    public function size() : ?int;
}
