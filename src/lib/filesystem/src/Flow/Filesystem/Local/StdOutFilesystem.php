<?php

declare(strict_types=1);

namespace Flow\Filesystem\Local;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Exception\InvalidSchemeException;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\StdOut\StdOutDestinationStream;
use Flow\Filesystem\Mount;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\SourceStream;
use Generator;
use php_user_filter;

use function array_key_exists;
use function Flow\Types\DSL\type_string;
use function mb_strtolower;
use function sprintf;

final class StdOutFilesystem implements Filesystem
{
    /** @var array<string, true> php:// targets currently held by an open destination stream */
    private array $heldTargets = [];

    public function __construct(
        private readonly Mount $mount = new Mount('stdout'),
        private readonly ?php_user_filter $filter = null,
    ) {}

    public function appendTo(Path $path): DestinationStream
    {
        if (!$this->mount->supports($path)) {
            throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);
        }

        $target = $this->resolveTarget($path);
        $this->acquireTarget($target);

        return new StdOutDestinationStream($path, $target, $this->filter, function () use ($target): void {
            $this->releaseTarget($target);
        });
    }

    public function getSystemTmpDir(): Path
    {
        throw new RuntimeException('StdOut does not have a system tmp directory');
    }

    public function list(Path $path, Filter $pathFilter = new KeepAll()): Generator
    {
        yield from [];
    }

    public function mount(): Mount
    {
        return $this->mount;
    }

    public function mv(Path $from, Path $to): bool
    {
        throw new RuntimeException('Cannot move files around in stdout');
    }

    public function readFrom(Path $path): SourceStream
    {
        throw new RuntimeException('Cannot read from stdout');
    }

    public function rm(Path $path): bool
    {
        throw new RuntimeException('Cannot remove files from stdout');
    }

    public function status(Path $path): ?FileStatus
    {
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

        $target = $this->resolveTarget($path);
        $this->acquireTarget($target);

        return new StdOutDestinationStream($path, $target, $this->filter, function () use ($target): void {
            $this->releaseTarget($target);
        });
    }

    private function acquireTarget(string $target): void
    {
        if (array_key_exists($target, $this->heldTargets)) {
            throw new RuntimeException(sprintf('Only one stream can be open at the same time for php://%s', $target));
        }

        $this->heldTargets[$target] = true;
    }

    private function releaseTarget(string $target): void
    {
        unset($this->heldTargets[$target]);
    }

    /**
     * @return 'output'|'stderr'|'stdout'
     */
    private function resolveTarget(Path $path): string
    {
        $target = mb_strtolower(type_string()->cast($path->getOption('stream', 'stdout')));

        return match ($target) {
            'stdout', 'stderr', 'output' => $target,
            default => throw new InvalidArgumentException('Invalid output stream, allowed values are "stdout", "stderr" and "output", given: '
            . $target),
        };
    }
}
