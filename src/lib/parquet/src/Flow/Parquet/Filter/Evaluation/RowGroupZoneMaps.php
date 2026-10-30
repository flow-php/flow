<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Evaluation;

use Flow\Parquet\ParquetFile\RowGroupChunks;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

use function array_key_exists;

final class RowGroupZoneMaps implements ZoneMaps
{
    /**
     * @var array<string, null|ZoneMap>
     */
    private array $zones = [];

    public function __construct(
        private readonly RowGroupChunks $chunks,
        private readonly int $rowGroup,
    ) {}

    public function zone(FlatColumn $column): ?ZoneMap
    {
        $path = $column->flatPath();

        if (!array_key_exists($path, $this->zones)) {
            $this->zones[$path] = $this->decode($column);
        }

        return $this->zones[$path];
    }

    private function decode(FlatColumn $column): ?ZoneMap
    {
        $statistics = $this->chunks->chunk($this->rowGroup, $column)->statistics();

        return $statistics === null
            ? null
            : ZoneMap::fromStatistics(
                column: $column,
                columnOrder: $this->chunks->columnOrder($column),
                min: $statistics->minValue($column),
                max: $statistics->maxValue($column),
                minExact: $statistics->isMinValueExact,
                maxExact: $statistics->isMaxValueExact,
                nullCount: $statistics->nullCount,
                nanCount: $statistics->nanCount,
                rowCount: $this->chunks->rowCounts()[$this->rowGroup],
            );
    }
}
