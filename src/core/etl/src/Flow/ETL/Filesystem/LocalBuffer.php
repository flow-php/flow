<?php declare(strict_types=1);

namespace Flow\ETL\Filesystem;

interface LocalBuffer
{
    public function release() : void;

    /**
     * @return resource
     */
    public function stream();

    public function write(string $data) : void;
}
