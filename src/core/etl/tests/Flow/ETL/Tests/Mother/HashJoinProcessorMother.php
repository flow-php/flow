<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Executor;
use Flow\ETL\Executor\PhysicalPlan;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Processor\HashJoinProcessor;
use Flow\ETL\Tests\Double\SpyBucketsStorage;

final class HashJoinProcessorMother
{
    /**
     * Non-resident in-memory storage - exercises the grace hash join path without disk I/O.
     *
     * @param int<1, max> $bucketsCount
     * @param int<1, max> $batchSize
     */
    public static function grace(
        PhysicalPlan $right,
        Expression $on,
        Join $type,
        ?SpyBucketsStorage $storage = null,
        int $bucketsCount = 64,
        int $batchSize = 1000,
    ): HashJoinProcessor {
        return self::with(
            $right,
            $on,
            $type,
            $storage ?? new SpyBucketsStorage(new MemoryBuckets()),
            $bucketsCount,
            $batchSize,
        );
    }

    /**
     * Resident storage - exercises the streaming, order-preserving fast path.
     *
     * @param int<1, max> $batchSize
     */
    public static function resident(
        PhysicalPlan $right,
        Expression $on,
        Join $type,
        int $batchSize = 1000,
    ): HashJoinProcessor {
        return self::with($right, $on, $type, new MemoryBuckets(), batchSize: $batchSize);
    }

    /**
     * @param int<1, max> $bucketsCount
     * @param int<1, max> $batchSize
     */
    public static function with(
        PhysicalPlan $right,
        Expression $on,
        Join $type,
        BucketsStorage $storage,
        int $bucketsCount = 64,
        int $batchSize = 1000,
    ): HashJoinProcessor {
        return new HashJoinProcessor(
            $right,
            new Executor(),
            $on,
            $type,
            new Buckets($storage),
            new Buckets($storage),
            new NativePHPRandomValueGenerator(),
            $bucketsCount,
            $batchSize,
        );
    }
}
