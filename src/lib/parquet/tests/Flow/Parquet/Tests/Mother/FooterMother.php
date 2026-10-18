<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Mother;

use Flow\Parquet\ParquetFile\ColumnOrder;
use Flow\Parquet\ParquetFile\RowGroup;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Statistics;
use Flow\Parquet\Tests\Double\SpyFooter;

use function array_map;
use function array_values;
use function count;
use function pack;

final class FooterMother
{
    /**
     * One row group of $rows rows, the columns' chunks without statistics, under TYPE_DEFINED_ORDER.
     */
    public static function oneGroup(int $rows, FlatColumn ...$columns): SpyFooter
    {
        return new SpyFooter(
            Schema::with(...$columns),
            $rows,
            [new RowGroup(array_map(static fn(FlatColumn $column) => ColumnChunkMother::withStatistics(
                $column,
                null,
            ), array_values($columns)), $rows)],
            array_map(static fn(): ColumnOrder => ColumnOrder::TypeDefined, array_values($columns)),
        );
    }

    /**
     * An INT64 column `id` in row groups of two rows, one [min, max] per group, under TYPE_DEFINED_ORDER.
     *
     * @param array{int, int} ...$bounds
     */
    public static function idGroups(array ...$bounds): SpyFooter
    {
        $id = FlatColumn::int64('id');

        return new SpyFooter(
            Schema::with($id),
            2 * count($bounds),
            array_map(
                static fn(array $bound): RowGroup => new RowGroup([
                    ColumnChunkMother::withStatistics(
                        $id,
                        new Statistics(null, null, 0, null, pack('P', $bound[1]), pack('P', $bound[0])),
                    ),
                ], 2),
                array_values($bounds),
            ),
            [ColumnOrder::TypeDefined],
        );
    }
}
