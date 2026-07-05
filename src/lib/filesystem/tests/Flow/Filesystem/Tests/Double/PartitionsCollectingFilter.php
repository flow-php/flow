<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Double;

use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Partitions;
use Flow\Filesystem\Path\Filter;

final class PartitionsCollectingFilter implements Filter
{
    /**
     * @var array<Partitions>
     */
    public array $partitionsList = [];

    public function accept(FileStatus $status): bool
    {
        $this->partitionsList[] = $status->path->partitions();

        return true;
    }
}
