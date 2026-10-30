<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Values\Schema;
use Flow\Filesystem\FileStatus;

interface ListedFileRow
{
    /**
     * @return array<string, mixed> the row of one listed file, its partition columns left out
     */
    public function row(FileStatus $status): array;

    /**
     * The body columns of a row, before the partition columns are declared.
     */
    public function schema(): Schema;
}
