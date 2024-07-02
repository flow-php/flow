<?php

declare(strict_types=1);

namespace Flow\Filesystem;

interface LocalBuffer
{
    public function release() : void;

    public function seek(int $offset, int $whence = SEEK_SET) : void;

    /**
     * @return resource
     */
    public function stream();

    public function tell() : int|false;

    public function write(string $data) : void;
}
