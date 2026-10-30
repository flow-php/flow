<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Evaluation;

use Flow\Parquet\Data\PhysicalCodec;
use Flow\Parquet\ParquetFile\PageIndex\PageIndex;
use Flow\Parquet\ParquetFile\RowGroupChunks;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

use function array_key_exists;

/**
 * One row group's page statistics, each page decoded once. at() views the pages holding one row, so columns whose pages
 * break at other rows are evaluated together over the rows their pages share.
 */
final class PageZoneMaps
{
    /**
     * @var array<string, array<int, ZoneMap>> by flat path, then page
     */
    private array $zones = [];

    private readonly PhysicalCodec $codec;

    /**
     * @param array<string, PageIndex> $pageIndexes by flat path, a column without one has no zone
     */
    public function __construct(
        private readonly RowGroupChunks $chunks,
        private readonly array $pageIndexes,
    ) {
        $this->codec = new PhysicalCodec();
    }

    public function at(int $row): ZoneMaps
    {
        return new PageZoneMapsAtRow($this, $row);
    }

    public function zone(FlatColumn $column, int $row): ?ZoneMap
    {
        $path = $column->flatPath();

        if (!array_key_exists($path, $this->pageIndexes)) {
            return null;
        }

        $pageIndex = $this->pageIndexes[$path];
        $page = $pageIndex->pageOf($row);

        return $this->zones[$path][$page] ??= $this->decode($column, $pageIndex, $page);
    }

    /**
     * Column index bounds carry no exactness flag.
     */
    private function decode(FlatColumn $column, PageIndex $pageIndex, int $page): ZoneMap
    {
        $columnIndex = $pageIndex->columnIndex;
        $rows = $pageIndex->pageRows($page);
        $nanCount = $columnIndex->nanCounts[$page] ?? null;

        if ($columnIndex->nullPages[$page]) {
            return new ZoneMap(null, null, true, true, $rows, $nanCount, $rows, false);
        }

        return ZoneMap::fromStatistics(
            column: $column,
            columnOrder: $this->chunks->columnOrder($column),
            min: $this->codec->decodeStatistic($column, $columnIndex->minValues[$page]),
            max: $this->codec->decodeStatistic($column, $columnIndex->maxValues[$page]),
            minExact: null,
            maxExact: null,
            nullCount: $columnIndex->nullCounts[$page] ?? null,
            nanCount: $nanCount,
            rowCount: $rows,
        );
    }
}
