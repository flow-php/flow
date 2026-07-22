<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Grouping;

use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Path;

interface GroupByAlgorithmBuilder
{
    public function build(FilesystemTable $filesystemTable, Path $localFilesystemCacheDir): HashGroupByConfig;
}
