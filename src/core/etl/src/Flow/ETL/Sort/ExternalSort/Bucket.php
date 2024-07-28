<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\ExternalSort;

use Flow\ETL\Row;

final class Bucket
{
    /**
     * @param string $id
     * @param iterable<Row> $rows
     */
    public function __construct(
        public readonly string $id,
        public readonly iterable $rows
    ) {
    }
}
