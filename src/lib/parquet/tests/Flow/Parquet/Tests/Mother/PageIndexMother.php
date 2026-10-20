<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Mother;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\PageIndex\BoundaryOrder;
use Flow\Parquet\ParquetFile\PageIndex\ColumnIndex;
use Flow\Parquet\ParquetFile\PageIndex\OffsetIndex;
use Flow\Parquet\ParquetFile\PageIndex\PageIndex;
use Flow\Parquet\ParquetFile\PageIndex\PageLocation;

use function array_fill;
use function array_map;
use function array_values;
use function count;
use function pack;

final class PageIndexMother
{
    /**
     * An INT64 column index, one [min, max] per page, null for a null page; no null in the other pages unless
     * $nullCounts says so.
     *
     * @param list<null|array{int, int}> $bounds
     * @param null|list<int> $nullCounts
     */
    public static function int64(array $bounds, ?array $nullCounts = null): ColumnIndex
    {
        return new ColumnIndex(
            array_map(static fn(?array $bound): bool => $bound === null, $bounds),
            array_map(static fn(?array $bound): string => $bound === null ? '' : pack('P', $bound[0]), $bounds),
            array_map(static fn(?array $bound): string => $bound === null ? '' : pack('P', $bound[1]), $bounds),
            BoundaryOrder::UNORDERED,
            $nullCounts ?? array_fill(0, count($bounds), 0),
        );
    }

    /**
     * Pages starting at the given rows of the row group, 10 bytes each from byte 100.
     */
    public static function pagesAt(int ...$firstRows): OffsetIndex
    {
        $pages = [];

        foreach (array_values($firstRows) as $page => $firstRow) {
            $pages[] = new PageLocation(100 + (10 * $page), 10, $firstRow);
        }

        return new OffsetIndex($pages);
    }

    /**
     * The consistent page index of a row group of $rows rows.
     */
    public static function consistent(ColumnIndex $columnIndex, OffsetIndex $offsetIndex, int $rows): PageIndex
    {
        return (
            PageIndex::of($columnIndex, $offsetIndex, $rows) ?? throw new InvalidArgumentException(
                'The column index and offset index are inconsistent',
            )
        );
    }
}
