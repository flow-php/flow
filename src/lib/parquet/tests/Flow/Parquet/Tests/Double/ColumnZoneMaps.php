<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Double;

use Flow\Parquet\Filter\Evaluation\ZoneMap;
use Flow\Parquet\Filter\Evaluation\ZoneMaps;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

/**
 * A zone per flat path; a column without one has no usable statistics.
 */
final readonly class ColumnZoneMaps implements ZoneMaps
{
    /**
     * @param array<string, ZoneMap> $zones
     */
    public function __construct(
        public array $zones,
    ) {}

    public function zone(FlatColumn $column): ?ZoneMap
    {
        return $this->zones[$column->flatPath()] ?? null;
    }
}
