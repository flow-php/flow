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

final class FailingCloseFilesystem implements Filesystem
{
    private int $opened = 0;

    public function __construct(
        private readonly Filesystem $inner,
        private readonly int $healthyStreams = 0,
        private readonly int $failingStreams = 1,
    ) {}

    public function appendTo(Path $path): DestinationStream
    {
        $stream = $this->inner->appendTo($path);
        $index = $this->opened++;

        return $index >= $this->healthyStreams && $index < ($this->healthyStreams + $this->failingStreams)
            ? new FailingCloseDestinationStream($stream)
            : $stream;
    }

    public function getSystemTmpDir(): Path
    {
        return $this->inner->getSystemTmpDir();
    }

    public function list(Path $path, Filter $pathFilter = new KeepAll()): Generator
    {
        yield from $this->inner->list($path, $pathFilter);
    }

    public function mount(): Mount
    {
        return $this->inner->mount();
    }

    public function mv(Path $from, Path $to): bool
    {
        return $this->inner->mv($from, $to);
    }

    public function readFrom(Path $path): SourceStream
    {
        return $this->inner->readFrom($path);
    }

    public function rm(Path $path): bool
    {
        return $this->inner->rm($path);
    }

    public function status(Path $path): ?FileStatus
    {
        return $this->inner->status($path);
    }

    public function supports(Path $path): bool
    {
        return $this->inner->supports($path);
    }

    public function writeTo(Path $path): DestinationStream
    {
        $stream = $this->inner->writeTo($path);
        $index = $this->opened++;

        return $index >= $this->healthyStreams && $index < ($this->healthyStreams + $this->failingStreams)
            ? new FailingCloseDestinationStream($stream)
            : $stream;
    }
}
