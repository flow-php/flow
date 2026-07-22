<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Bucketing;

use Flow\ETL\Bucketing\BucketsStorage;

final readonly class BucketingConfig
{
    /**
     * @param int<1, max> $bucketsCount
     * @param int<1, max> $batchSize
     */
    public function __construct(
        public BucketsStorage $storage,
        public int $bucketsCount,
        public int $batchSize,
    ) {}
}
