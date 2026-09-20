<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;

/**
 * FileReading for a source whose rows describe the files themselves: the same partition columns, no metadata
 * columns and no declared partition types - each row already is the file's metadata.
 */
trait ListingPartitions
{
    use PathFiltering;

    private function fileColumns(Filesystem $filesystem, Path $path): FileColumns
    {
        $partitionColumns = new PartitionColumns($filesystem);

        return new FileColumns(
            $partitionColumns,
            $this->partitionNames($partitionColumns, $path),
            new PartitionTypes(),
            false,
        );
    }
}
