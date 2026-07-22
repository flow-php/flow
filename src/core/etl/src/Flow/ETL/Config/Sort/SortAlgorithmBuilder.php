<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Sort;

use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Path;

interface SortAlgorithmBuilder
{
    public function build(
        FilesystemTable $filesystemTable,
        Path $localFilesystemCacheDir,
    ): MemorySortConfig|ExternalSortConfig;
}
