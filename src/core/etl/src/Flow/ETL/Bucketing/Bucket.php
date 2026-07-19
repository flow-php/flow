<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Row\References;

final readonly class Bucket
{
    public function __construct(
        public string $id,
        public References $by,
        public BucketStats $stats,
    ) {}
}
