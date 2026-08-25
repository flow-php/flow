<?php

declare(strict_types=1);

namespace Flow\Filesystem;

use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\PlaceholderPartitions;
use Generator;

final readonly class FileListing
{
    public function __construct(
        private Filesystem $filesystem,
    ) {}

    /**
     * $filter has no default on purpose - Filesystem::list() defaults it to KeepAll while
     * NativeLocalFilesystem::list() defaults it to OnlyFiles, so a default here would silently pick one.
     *
     * @return Generator<FileStatus>
     */
    public function list(Path $path, Filter $filter): Generator
    {
        $hasPlaceholders = [] !== $path->partitionPlaceholders();

        foreach ($this->filesystem->list(
            $path,
            $hasPlaceholders ? new PlaceholderPartitions($path, $filter) : $filter,
        ) as $status) {
            yield $hasPlaceholders
                ? new FileStatus(
                    $status->path->withPartitions($path->extractPlaceholderPartitions($status->path)),
                    $status->isFile(),
                    $status->size,
                    $status->lastModifiedAt,
                )
                : $status;
        }
    }
}
