<?php

declare(strict_types=1);

namespace Flow\Filesystem\Path\Filter;

use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;

/**
 * Attaches partitions extracted from partition placeholders in the pattern path to the file status
 * before delegating to the decorated filter. This way filters like partition pruning can see
 * partitions that are part of the file name instead of name=value directories.
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
