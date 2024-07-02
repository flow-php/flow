<?php

declare(strict_types=1);

namespace Flow\Filesystem;

interface Stream
{
    public function close() : void;

    public function isOpen() : bool;

    public function path() : Path;
}
