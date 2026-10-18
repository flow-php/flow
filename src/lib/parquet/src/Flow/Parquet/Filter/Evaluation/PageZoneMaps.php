<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Evaluation;

use Flow\Parquet\Data\PhysicalCodec;
use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\ParquetFile\ColumnOrder;
use Flow\Parquet\ParquetFile\PageIndex\PageIndex;
use Flow\Parquet\ParquetFile\RowGroupChunks;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

use function array_key_exists;

/**
 * Each column's zone is the page holding row $row of the row group, so columns whose pages break at other rows are
 * evaluated together over the rows their pages share.
 */
final class PageZoneMaps implements ZoneMaps
{
    /**
     * @var array<string, null|ZoneMap>
     */
    private array $zones = [];

    /**
     * @param array<string, PageIndex> $pageIndexes by flat path, a column without one has no zone
     */
    public function __construct(
        private readonly RowGroupChunks $chunks,
        private readonly array $pageIndexes,
        private readonly int $row,
    ) {}

    public function zone(FlatColumn $column): ?ZoneMap
    {
        $path = $column->flatPath();

        if (!array_key_exists($path, $this->zones)) {
            $this->zones[$path] = array_key_exists($path, $this->pageIndexes)
                ? $this->decode($column, $this->pageIndexes[$path])
                : null;
        }

        return $this->zones[$path];
    }

    /**
     * Column index bounds carry no exactness flag: BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY ones may be truncated.
     */
    private function decode(FlatColumn $column, PageIndex $pageIndex): ZoneMap
    {
        $columnIndex = $pageIndex->columnIndex;
        $page = $pageIndex->pageOf($this->row);
        $rows = $pageIndex->pageRows($page);
        $nanCount = $columnIndex->nanCounts[$page] ?? null;

        if ($columnIndex->nullPages[$page]) {
            return new ZoneMap(null, null, true, true, $rows, $nanCount, $rows, false);
        }

        $codec = new PhysicalCodec();
        $columnOrder = $this->chunks->columnOrder($column);
        $order = PhysicalOrder::forColumn($column);
        $min = $columnOrder->trustedMin($column, $codec->decodeStatistic($column, $columnIndex->minValues[$page]));
        $max = $columnOrder->trustedMax($column, $codec->decodeStatistic($column, $columnIndex->maxValues[$page]));
        $exact = $column->type() !== PhysicalType::BYTE_ARRAY && $column->type() !== PhysicalType::FIXED_LEN_BYTE_ARRAY;

        return new ZoneMap(
            $min,
            $max,
            $exact,
            $exact,
            $columnIndex->nullCounts[$page] ?? null,
            $nanCount,
            $rows,
            $columnOrder === ColumnOrder::Ieee754TotalOrder
            && ($min !== null && $order->isNan($min) || $max !== null && $order->isNan($max)),
        );
    }
}
