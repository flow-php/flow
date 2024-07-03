<?php

declare(strict_types=1);

namespace Flow\Filesystem;

interface Buffer
{
    public function append(string $data) : void;

    public function dump() : string;

    public function size() : int;
}
