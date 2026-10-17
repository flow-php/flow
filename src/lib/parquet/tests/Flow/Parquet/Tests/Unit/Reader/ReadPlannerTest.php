<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Reader;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader\ReadPlan;
use Flow\Parquet\Reader\ReadPlanner;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Reader\RowSelector;
use Flow\Parquet\Tests\Double\SpyFooter;
use Flow\Parquet\Tests\Mother\FooterMother;
use Flow\Parquet\Tests\Mother\ReadPlanMother;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\DSL\parquet_all;
use function Flow\Parquet\DSL\parquet_column;

final class ReadPlannerTest extends TestCase
{
    public function test_without_a_filter_the_window_is_read_and_matched_and_no_statistics_are_read(): void
    {
        $footer = new SpyFooter(Schema::with(FlatColumn::int64('id')), 10);

        static::assertEquals(ReadPlanMother::window(10, 2, 5), (new ReadPlanner())->plan($footer, null, 2, 5));
        static::assertSame([], $footer->rowGroupChunkReads);
    }

    public function test_a_limit_beside_a_filter_is_refused(): void
    {
        $footer = new SpyFooter(Schema::with(FlatColumn::int64('id')), 10);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'A limit counts the rows returned, which a plan filtered by statistics cannot honour',
        );

        (new ReadPlanner())->plan(
            $footer,
            BoundFilter::bind(parquet_column('id')->equals(1), $footer->schema(), new Options()),
            0,
            10,
        );
    }

    public function test_a_limit_of_zero_beside_a_filter_reads_no_row_and_no_footer_statistics(): void
    {
        $footer = new SpyFooter(Schema::with(FlatColumn::int64('id')), 10);

        static::assertEquals(
            ReadPlanMother::window(10, 0, 0),
            (new ReadPlanner())->plan(
                $footer,
                BoundFilter::bind(parquet_column('id')->equals(1), $footer->schema(), new Options()),
                limit: 0,
            ),
        );
        static::assertSame([], $footer->rowGroupChunkReads);
    }

    public function test_a_filter_asks_the_footer_for_the_bound_columns_only(): void
    {
        $id = FlatColumn::int64('id');
        $footer = new SpyFooter(Schema::with($id, FlatColumn::string('name')));

        (new ReadPlanner())->plan($footer, BoundFilter::bind(
            parquet_all(parquet_column('id')->greaterThan(1), parquet_column('missing')->isNull()),
            $footer->schema(),
            new Options(),
        ));

        static::assertEquals([[$id]], $footer->rowGroupChunkReads);
    }

    public function test_the_row_groups_statistics_narrow_the_window_from_its_offset_in_raw_rows(): void
    {
        $footer = FooterMother::idGroups([1, 2], [3, 4], [5, 6]);
        $filter = BoundFilter::bind(parquet_column('id')->greaterThan(2), $footer->schema(), new Options());
        $kept = new RowSelection(new RowSelector(2, true), new RowSelector(4, false));
        $fromThird = new RowSelection(new RowSelector(3, true), new RowSelector(3, false));

        static::assertEquals(new ReadPlan($kept, $kept), (new ReadPlanner())->plan($footer, $filter));
        static::assertEquals(new ReadPlan($fromThird, $fromThird), (new ReadPlanner())->plan($footer, $filter, 3));
    }
}
