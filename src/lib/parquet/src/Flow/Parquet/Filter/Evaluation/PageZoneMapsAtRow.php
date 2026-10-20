<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Evaluation;

use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final readonly class PageZoneMapsAtRow implements ZoneMaps
{
    public function __construct(
        private PageZoneMaps $pages,
        private int $row,
    ) {}

    public function zone(FlatColumn $column): ?ZoneMap
    {
        return $this->pages->zone($column, $this->row);
    }
}
