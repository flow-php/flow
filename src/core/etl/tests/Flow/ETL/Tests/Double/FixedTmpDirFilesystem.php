<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Mount;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\SourceStream;
use Generator;
use SensitiveParameter;

/**
 * Answers getSystemTmpDir() with a path the test chooses, so the default spill root is observable without
 * touching the real system tmp directory - which concurrent flow processes share.
 *
 */
final readonly class FixedTmpDirFilesystem implements Filesystem
{
    public function __construct(
        private Filesystem $inner,
        private Path $tmpDir,
    ) {}

    public static function fromArray(#[SensitiveParameter] array $configuration): static
    {
        throw new InvalidArgumentException(
            'FixedTmpDirFilesystem is a test double and cannot be rebuilt from a configuration',
        );
    }

    public function appendTo(Path $path): DestinationStream
    {
        return $this->inner->appendTo($path);
    }

    public function create(Path $path): DestinationStream
    {
        return $this->inner->create($path);
    }

    public function normalize(): ?array
    {
        return null;
    }

    public function getSystemTmpDir(): Path
    {
        return $this->tmpDir;
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

    public function rm(Path $path, bool $recursive = true): bool
    {
        return $this->inner->rm($path, $recursive);
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
