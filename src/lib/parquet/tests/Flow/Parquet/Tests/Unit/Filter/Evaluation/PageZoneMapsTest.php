<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Evaluation;

use Flow\Parquet\Filter\Evaluation\PageZoneMaps;
use Flow\Parquet\Filter\Evaluation\ZoneMap;
use Flow\Parquet\ParquetFile\ColumnOrder;
use Flow\Parquet\ParquetFile\MetadataRowGroupChunks;
use Flow\Parquet\ParquetFile\PageIndex\BoundaryOrder;
use Flow\Parquet\ParquetFile\PageIndex\ColumnIndex;
use Flow\Parquet\ParquetFile\RowGroup;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Double\SpyFooter;
use Flow\Parquet\Tests\Mother\ColumnChunkMother;
use Flow\Parquet\Tests\Mother\FooterMother;
use Flow\Parquet\Tests\Mother\PageIndexMother;
use PHPUnit\Framework\TestCase;

use function pack;

use const NAN;

final class PageZoneMapsTest extends TestCase
{
    public function test_a_zone_is_the_page_holding_the_row_its_rows_ending_where_the_next_page_begins(): void
    {
        $chunks = new MetadataRowGroupChunks(FooterMother::oneGroup(10, $a = FlatColumn::int64('a'))->metadata());
        $pageIndexes = ['a' => PageIndexMother::consistent(
            PageIndexMother::int64([[0, 3], [4, 6], [7, 9]], [0, 1, 0]),
            PageIndexMother::pagesAt(0, 4, 7),
            10,
        )];

        static::assertEquals(
            new ZoneMap(4, 6, true, true, 1, null, 3, false),
            (new PageZoneMaps($chunks, $pageIndexes, 5))->zone($a),
        );
        static::assertEquals(
            new ZoneMap(7, 9, true, true, 0, null, 3, false),
            (new PageZoneMaps($chunks, $pageIndexes, 9))->zone($a),
        );
    }

    public function test_a_null_page_is_all_null_whatever_its_bounds_hold(): void
    {
        $chunks = new MetadataRowGroupChunks(FooterMother::oneGroup(10, $a = FlatColumn::int64('a'))->metadata());

        static::assertEquals(
            new ZoneMap(null, null, true, true, 6, null, 6, false),
            (new PageZoneMaps(
                $chunks,
                ['a' => PageIndexMother::consistent(
                    PageIndexMother::int64([[0, 3], null], [0, 0]),
                    PageIndexMother::pagesAt(0, 4),
                    10,
                )],
                4,
            ))->zone($a),
        );
    }

    public function test_bytes_bounds_are_inexact(): void
    {
        $name = FlatColumn::string('name');
        $chunks = new MetadataRowGroupChunks(FooterMother::oneGroup(4, $name)->metadata());

        static::assertEquals(
            new ZoneMap('a', 'b', false, false, 0, null, 4, false),
            (new PageZoneMaps(
                $chunks,
                ['name' => PageIndexMother::consistent(
                    new ColumnIndex([false], ['a'], ['b'], BoundaryOrder::ASCENDING, [0]),
                    PageIndexMother::pagesAt(0),
                    4,
                )],
                0,
            ))->zone($name),
        );
    }

    public function test_nan_counts_are_kept_and_nan_bounds_under_the_total_order_mean_every_value_is_nan(): void
    {
        $ratio = FlatColumn::double('ratio');
        $chunks = new MetadataRowGroupChunks(
            (new SpyFooter(
                Schema::with($ratio),
                4,
                [new RowGroup([ColumnChunkMother::withStatistics($ratio, null)], 4)],
                [ColumnOrder::Ieee754TotalOrder],
            ))->metadata(),
        );
        $pageIndexes = ['ratio' => PageIndexMother::consistent(
            new ColumnIndex(
                [false, false],
                [pack('e', 1.5), pack('e', NAN)],
                [pack('e', 2.5), pack('e', NAN)],
                BoundaryOrder::UNORDERED,
                [0, 0],
                [1, 2],
            ),
            PageIndexMother::pagesAt(0, 2),
            4,
        )];

        static::assertEquals(
            new ZoneMap(1.5, 2.5, true, true, 0, 1, 2, false),
            (new PageZoneMaps($chunks, $pageIndexes, 0))->zone($ratio),
        );
        static::assertTrue((new PageZoneMaps($chunks, $pageIndexes, 3))->zone($ratio)?->allNaN);
    }

    public function test_bounds_an_order_does_not_trust_are_absent(): void
    {
        $a = FlatColumn::int64('a');
        $chunks = new MetadataRowGroupChunks((new SpyFooter(Schema::with($a), 4, [new RowGroup(
            [ColumnChunkMother::withStatistics($a, null)],
            4,
        )]))->metadata());

        static::assertEquals(
            new ZoneMap(null, null, true, true, 0, null, 4, false),
            (new PageZoneMaps(
                $chunks,
                ['a' => PageIndexMother::consistent(PageIndexMother::int64([[0, 3]]), PageIndexMother::pagesAt(0), 4)],
                0,
            ))->zone($a),
        );
    }

    public function test_a_column_without_a_page_index_has_no_zone(): void
    {
        $chunks = new MetadataRowGroupChunks(FooterMother::oneGroup(4, $a = FlatColumn::int64('a'))->metadata());

        static::assertNull((new PageZoneMaps($chunks, [], 0))->zone($a));
    }

    public function test_a_column_is_decoded_once(): void
    {
        $chunks = new MetadataRowGroupChunks(FooterMother::oneGroup(4, $a = FlatColumn::int64('a'))->metadata());
        $zones = new PageZoneMaps(
            $chunks,
            ['a' => PageIndexMother::consistent(PageIndexMother::int64([[0, 3]]), PageIndexMother::pagesAt(0), 4)],
            0,
        );

        static::assertSame($zones->zone($a), $zones->zone($a));
    }
}
