<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Foundation\NativePHPRandomValueGenerator;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\Processor\GroupByAggregationProcessor;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\Memory\Unit;

final class GroupByAggregationProcessorMother
{
    /**
     * A limit no test reaches - everything aggregates in one pass.
     */
    public static function inMemory(
        GroupBy $groupBy,
        RowsStorage $storage = new MemoryRowsStorage(),
    ): GroupByAggregationProcessor {
        return self::with($groupBy, $storage, Unit::fromGb(64));
    }

    /**
     * @param int<1, max> $bucketsCount
     * @param int<1, max> $batchSize
     */
    public static function with(
        GroupBy $groupBy,
        RowsStorage $storage,
        Unit $memoryLimit,
        int $bucketsCount = 4,
        int $batchSize = 1000,
    ): GroupByAggregationProcessor {
        return new GroupByAggregationProcessor(
            $groupBy,
            new BucketingConfig($storage, $bucketsCount, $batchSize, $memoryLimit),
            new NativePHPRandomValueGenerator(),
        );
    }
}
