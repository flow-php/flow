<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Sort;

use Flow\ETL\Config\Bucketing\BucketingConfig;

final readonly class ExternalSortConfig
{
    /**
     * @param int<1, max> $runSize - rows buffered and sorted in memory before they are spilled as one run
     */
    public function __construct(
        public BucketingConfig $bucketing,
        public int $runSize = 10_000,
    ) {}
}
