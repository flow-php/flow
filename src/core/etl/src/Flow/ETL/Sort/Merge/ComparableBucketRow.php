<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\Merge;

use Flow\ETL\Row;

final readonly class ComparableBucketRow
{
    /**
     * @param array<int, mixed> $values
     */
    public function __construct(
        public array $values,
        public Row $row,
        public string $bucketId,
    ) {}
}
