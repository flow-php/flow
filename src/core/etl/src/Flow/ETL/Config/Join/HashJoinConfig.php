<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Join;

use Flow\ETL\Config\Bucketing\BucketingConfig;
use Flow\ETL\Dataset\Memory\Unit;

final readonly class HashJoinConfig
{
    public function __construct(
        public BucketingConfig $bucketing,
        public Unit $memoryLimit,
    ) {}
}
