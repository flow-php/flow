<?php

declare(strict_types=1);

namespace Flow\Filesystem\Local;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Exception\InvalidArgumentException;
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
use SensitiveParameter;

use function rtrim;
use function str_starts_with;
use function strlen;
use function substr;
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

    public static function fromArray(#[SensitiveParameter] array $configuration): static
    {
        throw new InvalidArgumentException(
            'MemoryFilesystem holds in-process state and cannot be rebuilt from a configuration',
        );
    }

    public function appendTo(Path $path): DestinationStream
    {
        if (!$this->mount->supports($path)) {
            throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);
        }

        return $this->memory->for($path);
    }

    public function normalize(): ?array
    {
        return null;
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
        if (!$this->mount->supports($from)) {
            throw new InvalidSchemeException($from->protocol(), $this->mount->protocol);
        }

        if (!$this->mount->supports($to)) {
            throw new InvalidSchemeException($to->protocol(), $this->mount->protocol);
        }

        if ($this->memory->has($from)) {
            $content = $this->memory->for($from)->content();
            $this->memory->close($from);
            $this->writeTo($to)->append($content);

            return true;
        }

        // a path naming no file is a directory: every file under it moves, as in the local filesystem
        $directory = rtrim($from->uri(), '/') . '/';
        $target = rtrim($to->uri(), '/') . '/';
        $moved = false;

        foreach ($this->memory->paths() as $path) {
            if (str_starts_with($path->uri(), $directory)) {
                $moved =
                    $this->mv($path, Path::from($target . substr($path->uri(), strlen($directory)), $path->options()))
                    || $moved;
            }
        }

        return $moved;
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

        if (!$path->isPattern() && $this->memory->has($path)) {
            $this->memory->close($path);

            return true;
        }

        // a path naming no file is a directory: every file under it goes, as in the local filesystem
        $directory = rtrim($path->uri(), '/') . '/';
        $removed = false;

        foreach ($this->memory->paths() as $nextPath) {
            if ($path->isPattern() ? $path->matches($nextPath) : str_starts_with($nextPath->uri(), $directory)) {
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
