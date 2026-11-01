<?php

declare(strict_types=1);

namespace Flow\Filesystem\Path\Filter;

use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;

/**
 * Lets partition pruning see partitions from file-name placeholders, not only name=value directories.
 */
final readonly class PlaceholderPartitions implements Filter
{
    public function __construct(
        private Path $pattern,
        private Filter $filter,
    ) {}

    public function accept(FileStatus $status): bool
    {
        $partitions = $this->pattern->extractPlaceholderPartitions($status->path);

        if (!$partitions->count()) {
            return $this->filter->accept($status);
        }

        return $this->filter->accept(
            new FileStatus(
                $status->path->withPartitions($partitions),
                $status->isFile(),
                $status->size,
                $status->lastModifiedAt,
            ),
        );
    }
}
