<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\IO\Extractor\File\FileColumns;
use Flow\ETL\IO\Extractor\File\PartitionTypes;

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
        return new FileColumns($names, $types, $metadataColumns);
    }
}
