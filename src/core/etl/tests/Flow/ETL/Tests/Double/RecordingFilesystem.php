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
use Generator;

final class RecordingFilesystem implements Filesystem
{
    /** @var list<string> */
    public array $calls = [];

    public function __construct(
        private readonly Filesystem $filesystem,
    ) {}

    public function appendTo(Path $path): DestinationStream
    {
        $this->calls[] = 'appendTo';

        return new RecordingDestinationStream($this->filesystem->appendTo($path), $this);
    }

    public function getSystemTmpDir(): Path
    {
        return $this->filesystem->getSystemTmpDir();
    }

    public function list(Path $path, Filter $pathFilter = new KeepAll()): Generator
    {
        $this->calls[] = 'list';

        yield from $this->filesystem->list($path, $pathFilter);
    }

    public function mount(): Mount
    {
        return $this->filesystem->mount();
    }

    public function mv(Path $from, Path $to): bool
    {
        $this->calls[] = 'mv';

        return $this->filesystem->mv($from, $to);
    }

    public function readFrom(Path $path): SourceStream
    {
        $this->calls[] = 'readFrom';

        return new RecordingSourceStream($this->filesystem->readFrom($path), $this);
    }

    public function record(string $call): void
    {
        $this->calls[] = $call;
    }

    public function rm(Path $path): bool
    {
        $this->calls[] = 'rm';

        return $this->filesystem->rm($path);
    }

    public function status(Path $path): ?FileStatus
    {
        $this->calls[] = 'status';

        return $this->filesystem->status($path);
    }

    public function supports(Path $path): bool
    {
        return $this->filesystem->supports($path);
    }

    public function writeTo(Path $path): DestinationStream
    {
        $this->calls[] = 'writeTo';

        return new RecordingDestinationStream($this->filesystem->writeTo($path), $this);
    }
}
