<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Join;

use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Path;

interface JoinAlgorithmBuilder
{
    public function build(FilesystemTable $filesystemTable, Path $localFilesystemCacheDir): HashJoinConfig;
}
