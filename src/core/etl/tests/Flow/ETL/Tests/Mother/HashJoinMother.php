<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Foundation\NativePHPRandomValueGenerator;
use Flow\ETL\Functions\Join\Expression;
use Flow\ETL\Operators\Join\HashJoin;
use Flow\ETL\Operators\Join\HashJoinConfig;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Tests\Double\SpyRowsStorage;
use Flow\ETL\Values\Join\JoinType;
use Flow\ETL\Values\Memory\Unit;

final class HashJoinMother
{
    /**
     * Non-resident in-memory storage and a one-byte limit - exercises the grace hash join path without disk I/O.
     *
     * @param int<1, max> $bucketsCount
     * @param int<1, max> $batchSize
     */
    public static function grace(
        Expression $on,
        JoinType $type,
        ?SpyRowsStorage $storage = null,
        int $bucketsCount = 64,
        int $batchSize = 1000,
    ): HashJoin {
        return self::with(
            $on,
            $type,
            $storage ?? new SpyRowsStorage(new MemoryRowsStorage()),
            $bucketsCount,
            $batchSize,
            Unit::fromBytes(1),
        );
    }

    /**
     * Resident storage - exercises the streaming, order-preserving fast path.
     *
     * @param int<1, max> $batchSize
     */
    public static function resident(Expression $on, JoinType $type, int $batchSize = 1000): HashJoin
    {
        return self::with($on, $type, new MemoryRowsStorage(), batchSize: $batchSize);
    }

    /**
     * @param int<1, max> $bucketsCount
     * @param int<1, max> $batchSize
     */
    public static function with(
        Expression $on,
        JoinType $type,
        RowsStorage $storage,
        int $bucketsCount = 64,
        int $batchSize = 1000,
        ?Unit $memoryLimit = null,
    ): HashJoin {
        return new HashJoin(
            $on,
            $type,
            new HashJoinConfig(
                new BucketingConfig($storage, $bucketsCount, $batchSize, $memoryLimit ?? Unit::fromGb(1)),
                Unit::fromMb(10),
            ),
            new NativePHPRandomValueGenerator(),
        );
    }
}
