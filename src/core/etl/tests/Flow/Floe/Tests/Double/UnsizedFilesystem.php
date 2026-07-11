<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Double;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Mount;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\SourceStream;
use Generator;

final class UnsizedFilesystem implements Filesystem
{
    public function __construct(
        private readonly Filesystem $filesystem,
    ) {}

    public function appendTo(Path $path): DestinationStream
    {
        return $this->filesystem->appendTo($path);
    }

    public function getSystemTmpDir(): Path
    {
        return $this->filesystem->getSystemTmpDir();
    }

    public function list(Path $path, Filter $pathFilter = new KeepAll()): Generator
    {
        return $this->filesystem->list($path, $pathFilter);
    }

    public function mount(): Mount
    {
        return $this->filesystem->mount();
    }

    public function mv(Path $from, Path $to): bool
    {
        return $this->filesystem->mv($from, $to);
    }

    public function readFrom(Path $path): SourceStream
    {
        return new UnsizedSourceStream($this->filesystem->readFrom($path));
    }

    public function rm(Path $path): bool
    {
        return $this->filesystem->rm($path);
    }

    public function status(Path $path): ?FileStatus
    {
        return $this->filesystem->status($path);
    }

    public function writeTo(Path $path): DestinationStream
    {
        return $this->filesystem->writeTo($path);
    }
}
