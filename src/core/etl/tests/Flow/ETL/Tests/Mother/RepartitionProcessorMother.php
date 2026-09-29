<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Bucketing\HashBucketing;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Processor\RepartitionProcessor;
use Flow\ETL\Row\References;

final class RepartitionProcessorMother
{
    /**
     * A limit no test reaches - the input is held and grouped in one pass.
     */
    public static function inMemory(References $by, BucketsStorage $storage = new MemoryBuckets()): RepartitionProcessor
    {
        return self::with($by, $storage, Unit::fromGb(64));
    }

    /**
     * @param int<1, max> $bucketsCount
     */
    public static function with(
        References $by,
        BucketsStorage $storage,
        Unit $memoryLimit,
        int $bucketsCount = 4,
    ): RepartitionProcessor {
        return new RepartitionProcessor(
            $by,
            new HashBucketing(
                $by->all(),
                $bucketsCount,
                new NativeHasher(),
                new NativePHPRandomValueGenerator(),
                'repartition',
            ),
            new Buckets($storage),
            $memoryLimit,
        );
    }
}
