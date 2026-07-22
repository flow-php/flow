<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Sort;

use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Path;

final class MemorySortBuilder implements SortAlgorithmBuilder
{
    public function build(FilesystemTable $filesystemTable, Path $localFilesystemCacheDir): MemorySortConfig
    {
        return new MemorySortConfig();
    }
}
