<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Double;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Mount;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\SourceStream;
use Generator;

use function max;

/**
 * Serves every read as a LengthCapturingSourceStream - a non-local stream - over the wrapped filesystem's bytes.
 */
final class LengthCapturingFilesystem implements Filesystem
{
    public ?LengthCapturingSourceStream $lastStream = null;

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
        $stream = $this->wrapped->readFrom($path);

        return $this->lastStream = new LengthCapturingSourceStream(
            $stream->read(max(1, (int) $stream->size()), 0),
            $path,
        );
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
        return $this->wrapped->supports($path);
    }

    public function writeTo(Path $path): DestinationStream
    {
        return $this->wrapped->writeTo($path);
    }
}
