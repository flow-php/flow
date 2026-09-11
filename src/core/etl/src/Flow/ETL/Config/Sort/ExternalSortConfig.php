<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Sort;

use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Config\Bucketing\BucketingConfig;

final readonly class ExternalSortConfig
{
    /**
     * @param BucketingConfig $bucketing - the spill side
     * @param null|BucketsStorage $merge - null means the merged runs go to the same storage the spill uses
     * @param int<1, max> $runSize - rows buffered and sorted in memory before they are spilled as one run
     */
    public function __construct(
        public BucketingConfig $bucketing,
        public ?BucketsStorage $merge = null,
        public int $runSize = 10_000,
    ) {}
}
