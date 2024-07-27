<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\ExternalSort;

final class Config
{
    /**
     * @param int $sortBucketsCount - how many buckets are sorted in one run, 10 means that we are going to keep maximum 10 rows in memory at time
     * @param int $sortBucketMaxSize - how many rows are sorted in one bucket, higher number can reduce IO but increase memory consumption
     */
    public function __construct(
        public readonly int $sortBucketsCount = 10,
        public readonly int $sortBucketMaxSize = 500,
    ) {

    }
}
