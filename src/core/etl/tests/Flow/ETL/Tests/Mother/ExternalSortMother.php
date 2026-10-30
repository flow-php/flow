<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Foundation\NativePHPRandomValueGenerator;
use Flow\ETL\Kernels\Bucketing\Buckets;
use Flow\ETL\Kernels\Sort\ExternalSort;
use Flow\ETL\Kernels\Sort\ExternalSortConfig;
use Flow\ETL\Kernels\Sort\SortColumn;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Memory\Unit;

final class ExternalSortMother
{
    /**
     * A limit no test dataset reaches - everything sorts in memory.
     *
     * @param list<SortColumn> $columns
     * @param int<1, max> $batchSize
     */
    public static function inMemory(
        array $columns,
        RowsStorage $storage = new MemoryRowsStorage(),
        int $batchSize = 1000,
    ): ExternalSort {
        return self::with($columns, $storage, Unit::fromGb(1), batchSize: $batchSize);
    }

    /**
     * A one byte limit - every non-empty batch is spilled as its own sorted run.
     *
     * @param list<SortColumn> $columns
     * @param int<1, max> $mergeFanIn
     * @param int<1, max> $batchSize
     */
    public static function spilling(
        array $columns,
        RowsStorage $storage = new MemoryRowsStorage(),
        int $mergeFanIn = 10,
        int $batchSize = 1000,
    ): ExternalSort {
        return self::with($columns, $storage, Unit::fromBytes(1), $mergeFanIn, $batchSize);
    }

    /**
     * @param list<SortColumn> $columns
     * @param int<1, max> $mergeFanIn
     * @param int<1, max> $batchSize
     */
    public static function with(
        array $columns,
        RowsStorage $storage,
        Unit $memoryLimit,
        int $mergeFanIn = 10,
        int $batchSize = 1000,
        ?RowsStorage $mergeStorage = null,
    ): ExternalSort {
        return new ExternalSort(
            $columns,
            new Buckets($storage),
            new Buckets($mergeStorage ?? $storage),
            new NativePHPRandomValueGenerator(),
            $memoryLimit,
            $mergeFanIn,
            new BatchSize($batchSize),
        );
    }

    /**
     * The config inMemory() sorts under, for the processors built from a config.
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
     * The config spilling() sorts under, for the processors built from a config.
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
     * The config with() sorts under: the merge fan-in is the config's bucket count.
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
