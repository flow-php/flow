<?php

declare(strict_types=1);

namespace Flow\ETL\Sort\ExternalSort;

use Flow\ETL\Row;

final class CachedRow
{
    public function __construct(
        public readonly Row $row,
        public readonly string $generatorId
    ) {
    }
}
