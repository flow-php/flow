<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Row\References;
use Flow\ETL\Sort\ExternalSort;

final class ExternalSortMother
{
    /**
     * A limit no test dataset reaches - everything sorts in memory.
     *
     * @param int<1, max> $batchSize
     */
    public static function inMemory(
        References $refs,
        BucketsStorage $storage = new MemoryBuckets(),
        int $batchSize = 1000,
    ): ExternalSort {
        return self::with($refs, $storage, Unit::fromGb(1), batchSize: $batchSize);
    }

    /**
     * A one byte limit - every non-empty batch is spilled as its own sorted run.
     *
     * @param int<1, max> $mergeFanIn
     * @param int<1, max> $batchSize
     */
    public static function spilling(
        References $refs,
        BucketsStorage $storage = new MemoryBuckets(),
        int $mergeFanIn = 10,
        int $batchSize = 1000,
    ): ExternalSort {
        return self::with($refs, $storage, Unit::fromBytes(1), $mergeFanIn, $batchSize);
    }

    /**
     * @param int<1, max> $mergeFanIn
     * @param int<1, max> $batchSize
     */
    public static function with(
        References $refs,
        BucketsStorage $storage,
        Unit $memoryLimit,
        int $mergeFanIn = 10,
        int $batchSize = 1000,
        ?BucketsStorage $mergeStorage = null,
    ): ExternalSort {
        return new ExternalSort(
            $refs,
            new Buckets($storage),
            new Buckets($mergeStorage ?? $storage),
            new NativePHPRandomValueGenerator(),
            $memoryLimit,
            $mergeFanIn,
            $batchSize,
        );
    }
}
