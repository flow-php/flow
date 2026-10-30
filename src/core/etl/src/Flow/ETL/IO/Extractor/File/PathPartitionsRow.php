<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition\MapDefinition;
use Flow\ETL\Values\Schema\Definition\StringDefinition;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Partition;

use function array_map;
use function array_merge;
use function array_values;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;

final readonly class PathPartitionsRow implements ListedFileRow
{
    public function row(FileStatus $status): array
    {
        return [
            'path' => $status->path->uri(),
            'partitions' => array_merge(...array_values(array_map(static fn(Partition $p) => [
                $p->name => $p->value,
            ], $status->path->partitions()->toArray()))),
        ];
    }

    public function schema(): Schema
    {
        return new Schema(
            new StringDefinition('path'),
            new MapDefinition('partitions', type_map(type_string(), type_string())),
        );
    }
}
