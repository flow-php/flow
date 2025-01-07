<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\ExternalSort;

use Flow\ETL\Row;

final readonly class BucketRow
{
    public function __construct(
        public Row $row,
        public string $bucketId,
    ) {
    }
}
