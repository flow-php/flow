<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Sort;

use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Sort\SortAlgorithms;

final readonly class SortConfig
{
    /**
     * @param int<1, max> $bucketsCount - how many runs are merged at once during external sort
     * @param int<1, max> $bucketSize - rows buffered and sorted in memory before they are spilled as one run
     * @param int<1, max> $batchSize - number of rows per spill/output batch
     */
    public function __construct(
        public SortAlgorithms $algorithm,
        public BucketsStorage $cache,
        public int $bucketsCount = 100,
        public int $bucketSize = 10_000,
        public int $batchSize = 1000,
        public string $filesystemProtocol = 'file',
    ) {}
}
