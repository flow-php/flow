<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\Filesystem\FileListing;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Generator;

trait FileReading
{
    use DeclaresPartitionTypes;
    use MetadataColumns;
    use PathFiltering;

    private function fileColumns(Filesystem $filesystem, Path $path): FileColumns
    {
        $partitionColumns = new PartitionColumns($filesystem);

        return new FileColumns(
            $partitionColumns,
            $this->partitionNames($partitionColumns, $path),
            $this->declaredPartitionTypes(),
            $this->addMetadataColumns,
        );
    }

    /**
     * @return Generator<int, SourceFile>
     */
    private function sourceFiles(Filesystem $filesystem, Path $path): Generator
    {
        foreach ((new FileListing($filesystem))->list($path, $this->filter()) as $status) {
            yield new SourceFile($status->path);
        }
    }
}
