<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Double;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Mount;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\SourceStream;
use Generator;

final class ThrowingSourceFilesystem implements Filesystem
{
    public ?ThrowingSourceStream $lastStream = null;

    public function __construct(
        private readonly Filesystem $wrapped,
    ) {}

    public function appendTo(Path $path): DestinationStream
    {
        return $this->wrapped->appendTo($path);
    }

    public function getSystemTmpDir(): Path
    {
        return $this->wrapped->getSystemTmpDir();
    }

    public function list(Path $path, Filter $pathFilter = new KeepAll()): Generator
    {
        yield from $this->wrapped->list($path, $pathFilter);
    }

    public function mount(): Mount
    {
        return $this->wrapped->mount();
    }

    public function mv(Path $from, Path $to): bool
    {
        return $this->wrapped->mv($from, $to);
    }

    public function readFrom(Path $path): SourceStream
    {
        return $this->lastStream = new ThrowingSourceStream($path);
    }

    public function rm(Path $path): bool
    {
        return $this->wrapped->rm($path);
    }

    public function status(Path $path): ?FileStatus
    {
        return $this->wrapped->status($path);
    }

    public function supports(Path $path): bool
    {
        return $this->mount()->supports($path);
    }

    public function writeTo(Path $path): DestinationStream
    {
        return $this->wrapped->writeTo($path);
    }
}
