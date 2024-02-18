<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\FileStream;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\Partition\NoopFilter;
use Flow\ETL\Partition\PartitionFilter;

interface Filesystem
{
    public function directoryExists(Path $path) : bool;

    public function exists(Path $path) : bool;

    public function fileExists(Path $path) : bool;

    public function mv(Path $from, Path $to) : void;

    public function open(Path $path, Mode $mode) : FileStream;

    public function rm(Path $path) : void;

    /**
     * @return \Generator<Path>
     */
    public function scan(Path $path, PartitionFilter $partitionFilter = new NoopFilter()) : \Generator;
}
