<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Evaluation;

use Flow\Parquet\ParquetFile\Schema\FlatColumn;

interface ZoneMaps
{
    /**
     * Null when the unit has no usable statistics for the column.
     */
    public function zone(FlatColumn $column): ?ZoneMap;
}
