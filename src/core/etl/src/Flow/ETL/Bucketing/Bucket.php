<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Row;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\str_entry;

final readonly class Bucket
{
    public function __construct(
        public string $id,
        public int $totalRows,
        public int $index,
    ) {}

    public function toRow(): Row
    {
        return Row::create(
            str_entry(BucketShape::id->value, $this->id),
            int_entry(BucketShape::totalRows->value, $this->totalRows),
        );
    }
}
