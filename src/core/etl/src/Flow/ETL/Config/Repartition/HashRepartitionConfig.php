<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Repartition;

use Flow\ETL\Config\Bucketing\BucketingConfig;

final readonly class HashRepartitionConfig
{
    public function __construct(
        public BucketingConfig $bucketing,
    ) {}
}
