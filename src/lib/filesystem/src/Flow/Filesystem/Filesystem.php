<?php

declare(strict_types=1);

namespace Flow\Filesystem;

use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\KeepAll;
use Generator;

interface Filesystem
{
    public function appendTo(Path $path): DestinationStream;

    public function getSystemTmpDir(): Path;

    /**
     * @return \Generator<FileStatus>
     */
    public function list(Path $path, Filter $pathFilter = new KeepAll()): Generator;

    public function mount(): Mount;

    public function mv(Path $from, Path $to): bool;

    public function readFrom(Path $path): SourceStream;

    public function rm(Path $path): bool;

    public function status(Path $path): ?FileStatus;

    /**
     * True when this filesystem serves the given path's protocol.
     */
    public function supports(Path $path): bool;

    /**
     * Open destination stream for writing, if file already exists it will be overwritten.
     */
    public function writeTo(Path $path): DestinationStream;
}
