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

final class UnsizedStreamFilesystem implements Filesystem
{
    public function __construct(
        private readonly Filesystem $filesystem,
    ) {}

    public static function fromArray(#[SensitiveParameter] array $configuration): static
    {
        throw new InvalidArgumentException(
            'UnsizedFilesystem is a test double and cannot be rebuilt from a configuration',
        );
    }

    public function appendTo(Path $path): DestinationStream
    {
        return $this->filesystem->appendTo($path);
    }

    public function create(Path $path): DestinationStream
    {
        return $this->filesystem->create($path);
    }

    public function normalize(): ?array
    {
        return null;
    }

    public function getSystemTmpDir(): Path
    {
        return $this->filesystem->getSystemTmpDir();
    }

    public function list(Path $path, Filter $pathFilter = new KeepAll()): Generator
    {
        return $this->filesystem->list($path, $pathFilter);
    }

    public function mount(): Mount
    {
        return $this->filesystem->mount();
    }

    public function mv(Path $from, Path $to): bool
    {
        return $this->filesystem->mv($from, $to);
    }

    public function readFrom(Path $path): SourceStream
    {
        return new UnsizedSourceStream($this->filesystem->readFrom($path));
    }

    public function rm(Path $path, bool $recursive = true): bool
    {
        return $this->filesystem->rm($path, $recursive);
    }

    public function status(Path $path): ?FileStatus
    {
        return $this->filesystem->status($path);
    }

    public function supports(Path $path): bool
    {
        return $this->mount()->supports($path);
    }

    public function writeTo(Path $path): DestinationStream
    {
        return $this->filesystem->writeTo($path);
    }
}
