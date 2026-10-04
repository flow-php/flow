<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Sort;

use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Config\Bucketing\BucketingConfig;
use Flow\ETL\Dataset\Memory\Unit;

final readonly class ExternalSortConfig
{
    /**
     * @param BucketingConfig $bucketing - the spill side
     * @param Unit $memoryLimit - memory the sort holds (sorting in memory) before it spills sorted runs
     * @param null|BucketsStorage $merge - null means the merged runs go to the same storage the spill uses
     */
    public function __construct(
        public BucketingConfig $bucketing,
        public Unit $memoryLimit,
        public ?BucketsStorage $merge = null,
    ) {}
}
