<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Row;

final readonly class RowKey
{
    /**
     * @param list<mixed> $values extracted bucket-key values for this row, in reference order
     */
    public function __construct(
        public Row $row,
        public array $values,
    ) {}
}
