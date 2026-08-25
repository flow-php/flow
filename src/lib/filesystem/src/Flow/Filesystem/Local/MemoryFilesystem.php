<?php

declare(strict_types=1);

namespace Flow\Filesystem\Local;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Exception\InvalidSchemeException;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\Memory\Memory;
use Flow\Filesystem\Mount;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\SourceStream;
use Generator;
use php_user_filter;

use function usort;

final readonly class MemoryFilesystem implements Filesystem
{
    private Memory $memory;

    public function __construct(
        private Mount $mount = new Mount('memory'),
        ?php_user_filter $filter = null,
    ) {
        $this->memory = new Memory($filter);
    }

    public function appendTo(Path $path): DestinationStream
    {
        if (!$this->mount->supports($path)) {
            throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);
        }

        return $this->memory->for($path);
    }

    public function getSystemTmpDir(): Path
    {
        throw new RuntimeException('Memory does not have a system tmp directory');
    }

    public function list(Path $path, Filter $pathFilter = new KeepAll()): Generator
    {
        if (!$this->mount->supports($path)) {
            throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);
        }

        if (!$path->isPattern()) {
            if ($this->memory->has($path) && $pathFilter->accept($status = $this->statFor($path))) {
                yield $status;
            }

            return;
        }

        $paths = $this->memory->paths();

        usort($paths, static fn(Path $a, Path $b): int => $a->path() <=> $b->path());

        foreach ($paths as $nextPath) {
            if ($path->matches($nextPath) && $pathFilter->accept($status = $this->statFor($nextPath))) {
                yield $status;
            }
        }
    }

    public function mount(): Mount
    {
        return $this->mount;
    }

    public function mv(Path $from, Path $to): bool
    {
        throw new RuntimeException('Cannot move files around in memory');
    }

    public function readFrom(Path $path): SourceStream
    {
        if (!$this->mount->supports($path)) {
            throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);
        }

        if (!$this->memory->has($path)) {
            throw new RuntimeException('File not found in memory: ' . $path->uri());
        }

        return $this->memory->for($path);
    }

    public function rm(Path $path): bool
    {
        if (!$this->mount->supports($path)) {
            throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);
        }

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

    public function status(Path $path): ?FileStatus
    {
        if (!$this->mount->supports($path)) {
            throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);
        }

        if (!$path->isPattern()) {
            if (!$this->memory->has($path)) {
                return null;
            }

            return $this->statFor($path);
        }

        foreach ($this->memory->paths() as $nextPath) {
            if ($path->matches($nextPath)) {
                return $this->statFor($nextPath);
            }
        }

        return null;
    }

    public function supports(Path $path): bool
    {
        return $this->mount->supports($path);
    }

    public function writeTo(Path $path): DestinationStream
    {
        if (!$this->mount->supports($path)) {
            throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);
        }

        if ($this->status($path) !== null) {
            $this->memory->close($path);
        }

        return $this->memory->for($path);
    }

    private function statFor(Path $path): FileStatus
    {
        return new FileStatus($path, true, $this->memory->size($path), $this->memory->lastModifiedAt($path));
    }
}
