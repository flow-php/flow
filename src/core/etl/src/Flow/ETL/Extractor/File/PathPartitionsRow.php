<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\File;

use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Partition;

use function array_map;
use function array_merge;
use function array_values;

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
}
