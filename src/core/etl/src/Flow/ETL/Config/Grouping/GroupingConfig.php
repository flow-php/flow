<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Grouping;

use Flow\ETL\Bucketing\BucketsStorage;

final readonly class GroupingConfig
{
    /**
     * @param int<1, max> $bucketsCount
     * @param int<1, max> $batchSize
     */
    public function __construct(
        public BucketsStorage $storage,
        public int $bucketsCount = 64,
        public int $batchSize = 1000,
    ) {}
}
