<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\ExternalSort;

use Flow\ETL\Row;

final readonly class BucketRow
{
    /**
     * @param array<int, mixed> $sortValues
     */
    public function __construct(
        public Row $row,
        public string $bucketId,
        public array $sortValues,
    ) {}
}
