<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Writer\StatisticsLevel;
use Flow\Parquet\Writer\StatisticsOptions;
use PHPUnit\Framework\TestCase;

use function str_repeat;

final class StatisticsOptionsTest extends TestCase
{
    public function test_chunk_statistics_are_written_unless_the_level_is_none(): void
    {
        $id = FlatColumn::int64('id');

        foreach ([
            [StatisticsLevel::NONE,  false],
            [StatisticsLevel::CHUNK, true],
            [StatisticsLevel::PAGE,  true],
        ] as [$level, $written]) {
            $statistics = StatisticsOptions::fromOptions((new Options())->set(Option::STATISTICS_LEVEL, $level));
            $counter = $statistics->counter($id);
            $counter->addBatch([1]);

            static::assertSame($written, $statistics->chunk($counter) !== null);
        }
    }

    public function test_page_header_statistics_need_the_page_level_and_the_option(): void
    {
        $id = FlatColumn::int64('id');
        $options = static fn(
            StatisticsLevel $level,
            bool $header,
        ): StatisticsOptions => StatisticsOptions::fromOptions((new Options())->set(
            Option::STATISTICS_LEVEL,
            $level,
        )->set(Option::PAGE_HEADER_STATISTICS, $header));
        $counter = $options(StatisticsLevel::PAGE, true)->counter($id);
        $counter->addBatch([1]);

        static::assertNotNull($options(StatisticsLevel::PAGE, true)->pageHeader($counter));
        static::assertNull($options(StatisticsLevel::PAGE, false)->pageHeader($counter));
        static::assertNull($options(StatisticsLevel::CHUNK, true)->pageHeader($counter));
    }

    public function test_each_page_index_is_new_and_truncates_to_the_column_index_length(): void
    {
        $name = FlatColumn::string('name');
        $statistics = StatisticsOptions::fromOptions((new Options())->set(Option::COLUMN_INDEX_TRUNCATE_LENGTH, 4));
        $page = $statistics->counter($name);
        $page->addBatch([str_repeat('a', 10)]);
        $first = $statistics->pageIndex($name);
        $first->addPage($page, 1);

        static::assertNotSame($first, $statistics->pageIndex($name));
        static::assertNull($statistics->pageIndex($name)->columnIndex());
        static::assertSame(['aaaa'], $first->columnIndex()?->minValues);
    }

    public function test_a_column_index_truncate_length_below_one_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('COLUMN_INDEX_TRUNCATE_LENGTH must be greater than 0, 0 given');

        StatisticsOptions::fromOptions((new Options())->set(Option::COLUMN_INDEX_TRUNCATE_LENGTH, 0));
    }
}
