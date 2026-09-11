<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Extractor\FileColumns;
use Flow\ETL\Extractor\PartitionColumns;
use Flow\ETL\Extractor\PartitionTypes;

use function Flow\Filesystem\DSL\memory_filesystem;

final class FileColumnsContext
{
    /**
     * @param array<string, bool> $names partition name => nullable
     */
    public static function discovering(
        array $names = ['year' => false],
        PartitionTypes $types = new PartitionTypes(),
        bool $metadataColumns = false,
    ): FileColumns {
        return new FileColumns(new PartitionColumns(memory_filesystem()), $names, $types, $metadataColumns);
    }
}
