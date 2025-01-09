<?php

declare(strict_types=1);

namespace Flow\Filesystem;

final readonly class FileStatus
{
    public function __construct(
        public Path $path,
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
