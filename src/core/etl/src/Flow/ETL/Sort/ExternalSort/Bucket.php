<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\ExternalSort;

use Flow\ETL\Row;

final readonly class Bucket
{
    /**
     * @param string $id
     * @param iterable<Row> $rows
     */
    public function __construct(
        public string $id,
        public iterable $rows,
    ) {
    }
}
