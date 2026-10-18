<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer;

use Flow\Parquet\ParquetFile\PageIndex\BoundaryOrder;
use Flow\Parquet\ParquetFile\PageIndex\ColumnIndex;
use Flow\Parquet\ParquetFile\PageIndex\OffsetIndex;
use Flow\Parquet\ParquetFile\PageIndex\PageLocation;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Tests\Mother\PageContainerMother;
use Flow\Parquet\Tests\Mother\StatisticsCounterMother;
use Flow\Parquet\Writer\PageContainers;
use Flow\Parquet\Writer\PageIndexBuilder;
use Flow\Parquet\Writer\StatisticsLevel;
use Flow\Parquet\Writer\StatisticsTruncation;
use PHPUnit\Framework\TestCase;

use function pack;
use function str_repeat;
use function unpack;

use const NAN;

final class PageIndexBuilderTest extends TestCase
{
    public function test_each_page_is_one_entry_and_a_page_without_values_is_a_null_page(): void
    {
        $id = FlatColumn::int64('id');
        $builder = new PageIndexBuilder($id, StatisticsLevel::PAGE, new StatisticsTruncation(64));

        $builder->addPage(StatisticsCounterMother::counted($id, [1, null, 3]), 3);
        $builder->addPage(StatisticsCounterMother::counted($id, [null, null]), 2);
        $builder->addPage(StatisticsCounterMother::counted($id, [5, 9]), 2);

        static::assertEquals(
            new ColumnIndex(
                [false, true, false],
                [pack('P', 1), '', pack('P', 5)],
                [pack('P', 3), '', pack('P', 9)],
                BoundaryOrder::ASCENDING,
                [1, 2, 0],
            ),
            $builder->columnIndex(),
        );
    }

    public function test_the_boundary_order_follows_the_pages_bounds(): void
    {
        $id = FlatColumn::int64('id');
        $descending = new PageIndexBuilder($id, StatisticsLevel::PAGE, new StatisticsTruncation(64));
        $descending->addPage(StatisticsCounterMother::counted($id, [5, 9]), 2);
        $descending->addPage(StatisticsCounterMother::counted($id, [1, 3]), 2);
        $unordered = new PageIndexBuilder($id, StatisticsLevel::PAGE, new StatisticsTruncation(64));
        $unordered->addPage(StatisticsCounterMother::counted($id, [5, 9]), 2);
        $unordered->addPage(StatisticsCounterMother::counted($id, [1, 3]), 2);
        $unordered->addPage(StatisticsCounterMother::counted($id, [4, 10]), 2);

        static::assertSame(BoundaryOrder::DESCENDING, $descending->columnIndex()?->boundaryOrder);
        static::assertSame(BoundaryOrder::UNORDERED, $unordered->columnIndex()?->boundaryOrder);
    }

    public function test_float_pages_count_nan_and_an_all_nan_page_has_nan_bounds(): void
    {
        $ratio = FlatColumn::double('ratio');
        $builder = new PageIndexBuilder($ratio, StatisticsLevel::PAGE, new StatisticsTruncation(64));

        $builder->addPage(StatisticsCounterMother::counted($ratio, [1.5, NAN]), 2);
        $builder->addPage(StatisticsCounterMother::counted($ratio, [NAN]), 1);
        $index = $builder->columnIndex();

        static::assertSame([1, 1], $index?->nanCounts);
        static::assertNan(unpack('e', $index->minValues[1])[1]);
        $id = FlatColumn::int64('id');
        $integers = new PageIndexBuilder($id, StatisticsLevel::PAGE, new StatisticsTruncation(64));
        $integers->addPage(StatisticsCounterMother::counted($id, [1]), 1);

        static::assertNull($integers->columnIndex()?->nanCounts);
    }

    public function test_bytes_bounds_are_truncated_to_the_column_index_length(): void
    {
        $name = FlatColumn::string('name');
        $builder = new PageIndexBuilder($name, StatisticsLevel::PAGE, new StatisticsTruncation(4));

        $builder->addPage(StatisticsCounterMother::counted($name, [str_repeat('a', 10), str_repeat('b', 10)]), 2);

        static::assertSame(['aaaa'], $builder->columnIndex()?->minValues);
        static::assertSame(['bbbc'], $builder->columnIndex()?->maxValues);
    }

    public function test_a_page_with_values_but_no_bounds_drops_the_column_index(): void
    {
        $timestamp = new FlatColumn('timestamp', PhysicalType::INT96);
        $builder = new PageIndexBuilder($timestamp, StatisticsLevel::PAGE, new StatisticsTruncation(64));

        $builder->addPage(StatisticsCounterMother::counted($timestamp, [str_repeat("\x01", 12)]), 1);

        static::assertNull($builder->columnIndex());
    }

    public function test_only_the_page_level_writes_a_column_index(): void
    {
        $id = FlatColumn::int64('id');

        foreach ([StatisticsLevel::NONE, StatisticsLevel::CHUNK] as $level) {
            $builder = new PageIndexBuilder($id, $level, new StatisticsTruncation(64));
            $builder->addPage(StatisticsCounterMother::counted($id, [1]), 1);

            static::assertNull($builder->columnIndex());
        }
    }

    public function test_the_offset_index_locates_each_data_page_and_its_first_row(): void
    {
        $id = FlatColumn::int64('id');
        $builder = new PageIndexBuilder($id, StatisticsLevel::NONE, new StatisticsTruncation(64));
        $first = PageContainerMother::dataPage(3);
        $second = PageContainerMother::dataPage(2);

        $builder->addPage(StatisticsCounterMother::counted($id, [1, 2, 3]), 3);
        $builder->addPage(StatisticsCounterMother::counted($id, [4, 5]), 2);

        static::assertEquals(
            new OffsetIndex([
                new PageLocation(100, $first->totalCompressedSize(), 0),
                new PageLocation(100 + $first->totalCompressedSize(), $second->totalCompressedSize(), 3),
            ]),
            $builder->offsetIndex(100, new PageContainers([$first, $second])),
        );
    }
}
