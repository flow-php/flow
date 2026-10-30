<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Loader\File;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\IO\Filesystem\SaveMode;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\Partitioning;
use Flow\Filesystem\Path;

interface FileLoader extends Loader
{
    /**
     * The job commit of this write as $tasks tasks of run $run; null when the save mode does not admit the destination.
     *
     * @param non-empty-string $run
     * @param int<1, max> $tasks
     *
     * @throws RuntimeException
     */
    public function commit(string $run, int $tasks): ?FileCommit;

    public function destination(): Path;

    /**
     * A new loader, every setting copied, writing as task $task of a job that commits it; this instance is untouched.
     */
    public function inTask(Part $task): static;

    public function partitionBy(Partitioning $partitioning): static;

    /**
     * What partitionBy() set; Partitioning::none() by default.
     */
    public function partitioning(): Partitioning;

    public function saveMode(SaveMode $mode): static;
}
