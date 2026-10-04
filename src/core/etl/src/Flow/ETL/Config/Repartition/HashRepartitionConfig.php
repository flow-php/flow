<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Repartition;

use Flow\ETL\Config\Bucketing\BucketingConfig;
use Flow\ETL\Dataset\Memory\Unit;

final readonly class HashRepartitionConfig
{
    public function __construct(
        public BucketingConfig $bucketing,
        public Unit $memoryLimit,
    ) {}
}
