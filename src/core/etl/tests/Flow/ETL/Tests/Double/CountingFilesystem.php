<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Mount;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\SourceStream;
use Flow\Floe\Tests\Double\ClosingSpySourceStream;
use Generator;

/**
 * Counts readFrom() and list() calls so a test can assert how many times a listed file is opened,
 * and hands back streams that record their own close() so opens and closes can be compared.
 */
final class CountingFilesystem implements Filesystem
{
    public int $listCalls = 0;

    public int $readFromCalls = 0;

    /**
     * @var list<ClosingSpySourceStream>
     */
    public array $openedStreams = [];

    public function __construct(
        private readonly Filesystem $inner,
    ) {}

    public function appendTo(Path $path): DestinationStream
    {
        return $this->inner->appendTo($path);
    }

    public function getSystemTmpDir(): Path
    {
        return $this->inner->getSystemTmpDir();
    }

    public function closedStreams(): int
    {
        $closed = 0;

        foreach ($this->openedStreams as $stream) {
            $closed += $stream->closeCount > 0 ? 1 : 0;
        }

        return $closed;
    }

    public function list(Path $path, Filter $pathFilter = new KeepAll()): Generator
    {
        $this->listCalls++;

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
        $this->readFromCalls++;

        return $this->openedStreams[] = new ClosingSpySourceStream($this->inner->readFrom($path));
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
        return $this->inner->writeTo($path);
    }
}
