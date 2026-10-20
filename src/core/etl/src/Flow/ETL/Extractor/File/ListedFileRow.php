<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\File;

use Flow\Filesystem\FileStatus;

interface ListedFileRow
{
    /**
     * @return array<string, mixed> the row of one listed file, its partition columns left out
     */
    public function row(FileStatus $status): array;
}
