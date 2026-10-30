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

use function in_array;

/**
 * Lists the given URIs without a size, the way NativeLocalFilesystem lists a zero-byte file.
 *
 */
final readonly class UnsizedFilesystem implements Filesystem
{
    /**
     * @param list<string> $unsizedUris
     */
    public function __construct(
        private Filesystem $inner,
        private array $unsizedUris,
    ) {}

    public static function fromArray(#[SensitiveParameter] array $configuration): static
    {
        throw new InvalidArgumentException(
            'UnsizedFilesystem is a test double and cannot be rebuilt from a configuration',
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
        return $this->inner->getSystemTmpDir();
    }

    public function list(Path $path, Filter $pathFilter = new KeepAll()): Generator
    {
        foreach ($this->inner->list($path, $pathFilter) as $status) {
            yield in_array($status->path->uri(), $this->unsizedUris, true)
                ? new FileStatus($status->path, $status->isFile(), null, $status->lastModifiedAt)
                : $status;
        }
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
