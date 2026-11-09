<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Foundation\NativePHPRandomValueGenerator;
use Flow\ETL\Foundation\Progress;
use Flow\ETL\Foundation\VoidProgress;
use Flow\ETL\Kernels\Bucketing\Buckets;
use Flow\ETL\Kernels\Sort\ExternalSort;
use Flow\ETL\Kernels\Sort\ExternalSortConfig;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Memory\Unit;
use Flow\ETL\Values\Sort\SortColumn;

final class ExternalSortMother
{
    /**
     * @param list<SortColumn> $columns
     * @param int<1, max> $mergeFanIn
     * @param int<1, max> $batchSize
     */
    public static function with(
        array $columns,
        RowsStorage $storage = new MemoryRowsStorage(),
        int $mergeFanIn = 10,
        int $batchSize = 1000,
        ?RowsStorage $mergeStorage = null,
        Progress $progress = new VoidProgress(),
    ): ExternalSort {
        return new ExternalSort(
            $columns,
            new Buckets($storage),
            new Buckets($mergeStorage ?? $storage),
            new NativePHPRandomValueGenerator(),
            $mergeFanIn,
            new BatchSize($batchSize),
            $progress,
        );
    }

    /**
     * A limit no test dataset reaches - everything sorts in memory.
     *
     * @param int<1, max> $batchSize
     */
    public static function inMemoryConfig(
        RowsStorage $storage = new MemoryRowsStorage(),
        int $batchSize = 1000,
    ): ExternalSortConfig {
        return self::config($storage, Unit::fromGb(1), batchSize: $batchSize);
    }

    /**
     * A one byte limit - every non-empty batch is spilled as its own sorted run.
     *
     * @param int<1, max> $mergeFanIn
     * @param int<1, max> $batchSize
     */
    public static function spillingConfig(
        RowsStorage $storage = new MemoryRowsStorage(),
        int $mergeFanIn = 10,
        int $batchSize = 1000,
    ): ExternalSortConfig {
        return self::config($storage, Unit::fromBytes(1), $mergeFanIn, $batchSize);
    }

    /**
     * The merge fan-in is the config's bucket count.
     *
     * @param int<1, max> $mergeFanIn
     * @param int<1, max> $batchSize
     */
    public static function config(
        RowsStorage $storage,
        Unit $memoryLimit,
        int $mergeFanIn = 10,
        int $batchSize = 1000,
        ?RowsStorage $mergeStorage = null,
    ): ExternalSortConfig {
        return new ExternalSortConfig(
            new BucketingConfig($storage, $mergeFanIn, $batchSize, $memoryLimit),
            $mergeStorage,
        );
    }
}
