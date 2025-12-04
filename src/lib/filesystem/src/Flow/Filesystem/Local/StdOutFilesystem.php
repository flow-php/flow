<?php

declare(strict_types=1);

namespace Flow\Filesystem\Local;

use Flow\Filesystem\{DestinationStream, FileStatus, Filesystem, Path, Protocol, SourceStream};
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Local\StdOut\StdOutDestinationStream;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\KeepAll;

final readonly class StdOutFilesystem implements Filesystem
{
    public function __construct(private ?\php_user_filter $filter = null)
    {
    }

    #[\Override]
    public function appendTo(Path $path) : DestinationStream
    {
        $this->protocol()->validateScheme($path);

        return new StdOutDestinationStream($path, $this->filter);
    }

    #[\Override]
    public function getSystemTmpDir() : Path
    {
        throw new RuntimeException('StdOut does not have a system tmp directory');
    }

    #[\Override]
    public function list(Path $path, Filter $pathFilter = new KeepAll()) : \Generator
    {
        yield from [];
    }

    #[\Override]
    public function mv(Path $from, Path $to) : bool
    {
        throw new RuntimeException('Cannot move files around in stdout');
    }

    #[\Override]
    public function protocol() : Protocol
    {
        return new Protocol('stdout');
    }

    #[\Override]
    public function readFrom(Path $path) : SourceStream
    {
        throw new RuntimeException('Cannot read from stdout');
    }

    #[\Override]
    public function rm(Path $path) : bool
    {
        throw new RuntimeException('Cannot read from stdout');
    }

    #[\Override]
    public function status(Path $path) : ?FileStatus
    {
        return null;
    }

    #[\Override]
    public function writeTo(Path $path) : DestinationStream
    {
        $this->protocol()->validateScheme($path);

        return new StdOutDestinationStream($path, $this->filter);
    }
}
