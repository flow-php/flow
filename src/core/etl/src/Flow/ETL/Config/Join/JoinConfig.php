<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Join;

use Flow\ETL\Bucketing\BucketsStorage;

final readonly class JoinConfig
{
    /**
     * @param int<1, max> $bucketsCount
     * @param int<1, max> $batchSize
     */
    public function __construct(
        public BucketsStorage $cache,
        public int $bucketsCount = 64,
        public int $batchSize = 1000,
    ) {}
}
