<?php

declare(strict_types=1);

namespace Flow\Filesystem;

final class FileStatus
{
    public function __construct(
        public readonly Path $path,
        private bool $isFile,
    ) {

    }

    public function isDirectory() : bool
    {
        return !$this->isFile;
    }

    public function isFile() : bool
    {
        return $this->isFile;
    }
}
