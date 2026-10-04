<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Grouping;

use Flow\ETL\Config\Bucketing\BucketingConfig;
use Flow\ETL\Dataset\Memory\Unit;

final readonly class HashGroupByConfig
{
    public function __construct(
        public BucketingConfig $bucketing,
        public Unit $memoryLimit,
    ) {}
}
