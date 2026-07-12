<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Grouping;

use Flow\ETL\Sort\ExternalSort\BucketsCache;

final readonly class GroupingConfig
{
    /**
     * @param null|BucketsCache $cache null keeps the aggregation fully in memory
     * @param int<1, max> $partitions
     * @param int<1, max> $batchSize
     */
    public function __construct(
        public ?BucketsCache $cache,
        public int $partitions = 64,
        public int $batchSize = 1000,
    ) {}
}
