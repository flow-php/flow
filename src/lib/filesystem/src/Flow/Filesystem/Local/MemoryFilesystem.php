<?php

declare(strict_types=1);

namespace Flow\Filesystem\Local;

use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\{DestinationStream,
    FileStatus,
    Filesystem,
    Local\Memory\Memory,
    Path,
    Protocol,
    SourceStream};

final readonly class MemoryFilesystem implements Filesystem
{
    private Memory $memory;

    public function __construct(?\php_user_filter $filter = null)
    {
        $this->memory = new Memory($filter);
    }

    public function appendTo(Path $path) : DestinationStream
    {
        $this->protocol()->validateScheme($path);

        return $this->memory->for($path);
    }

    public function getSystemTmpDir() : Path
    {
        throw new RuntimeException('Memory does not have a system tmp directory');
    }

    public function list(Path $path, Filter $pathFilter = new KeepAll()) : \Generator
    {
        $this->protocol()->validateScheme($path);

        if (!$path->isPattern()) {
            if ($this->memory->has($path) && $pathFilter->accept($status = new FileStatus($path, true))) {
                yield $status;
            }

            return;

        }

        $paths = $this->memory->paths();

        \usort($paths, static fn (Path $a, Path $b) : int => $a->path() <=> $b->path());

        foreach ($paths as $nextPath) {
            if ($path->matches($nextPath) && $pathFilter->accept($status = new FileStatus($nextPath, true))) {
                yield $status;
            }
        }
    }

    public function mv(Path $from, Path $to) : bool
    {
        throw new RuntimeException('Cannot move files around in memory');
    }

    public function protocol() : Protocol
    {
        return new Protocol('memory');
    }

    public function readFrom(Path $path) : SourceStream
    {
        $this->protocol()->validateScheme($path);

        if (!$this->memory->has($path)) {
            throw new RuntimeException('File not found in memory: ' . $path->uri());
        }

        return $this->memory->for($path);
    }

    public function rm(Path $path) : bool
    {
        $this->protocol()->validateScheme($path);

        if (!$path->isPattern()) {
            if (!$this->memory->has($path)) {
                return false;
            }

            $this->memory->close($path);

            return true;
        }

        $removed = false;

        foreach ($this->memory->paths() as $nextPath) {
            if ($path->matches($nextPath)) {
                $this->memory->close($nextPath);
                $removed = true;
            }
        }

        return $removed;
    }

    public function status(Path $path) : ?FileStatus
    {
        $this->protocol()->validateScheme($path);

        if (!$path->isPattern()) {
            if (!$this->memory->has($path)) {
                return null;
            }

            return new FileStatus($path, true);
        }

        foreach ($this->memory->paths() as $nextPath) {
            if ($path->matches($nextPath)) {
                return new FileStatus($nextPath, true);
            }
        }

        return null;
    }

    public function writeTo(Path $path) : DestinationStream
    {
        $this->protocol()->validateScheme($path);

        if ($this->status($path) !== null) {
            $this->memory->close($path);
        }

        return $this->memory->for($path);
    }
}
