<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\FileStream;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\Partition\PartitionFilter;
use Flow\Serializer\Serializable;

/**
 * @template T
 * @extends Serializable<T>
 */
interface Filesystem extends Serializable
{
    public function open(Path $path, Mode $mode) : FileStream;

    /**
     * @return \Generator<int, Path>
     */
    public function scan(Path $path, PartitionFilter $partitionFilter) : \Generator;
}
