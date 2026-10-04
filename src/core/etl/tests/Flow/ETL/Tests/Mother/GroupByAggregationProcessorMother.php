<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Bucketing\HashBucketing;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\GroupBy;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Processor\GroupByAggregationProcessor;

final class GroupByAggregationProcessorMother
{
    /**
     * A limit no test reaches - everything aggregates in one pass.
     */
    public static function inMemory(
        GroupBy $groupBy,
        BucketsStorage $storage = new MemoryBuckets(),
    ): GroupByAggregationProcessor {
        return self::with($groupBy, $storage, Unit::fromGb(64));
    }

    /**
     * @param int<1, max> $bucketsCount
     * @param int<1, max> $batchSize
     */
    public static function with(
        GroupBy $groupBy,
        BucketsStorage $storage,
        Unit $memoryLimit,
        int $bucketsCount = 4,
        int $batchSize = 1000,
    ): GroupByAggregationProcessor {
        return new GroupByAggregationProcessor(
            $groupBy,
            new HashBucketing(
                $groupBy->references(),
                $bucketsCount,
                new NativeHasher(),
                new NativePHPRandomValueGenerator(),
                'group-by',
            ),
            new Buckets($storage),
            $memoryLimit,
            $batchSize,
        );
    }
}
