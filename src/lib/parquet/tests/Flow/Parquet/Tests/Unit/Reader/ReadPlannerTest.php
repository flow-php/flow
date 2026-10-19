<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Reader;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Filter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader\ReadPlan;
use Flow\Parquet\Reader\ReadPlanner;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Reader\RowSelector;
use Flow\Parquet\Tests\Double\InMemoryIndexes;
use Flow\Parquet\Tests\Double\SpyFooter;
use Flow\Parquet\Tests\Mother\FooterMother;
use Flow\Parquet\Tests\Mother\PageIndexMother;
use Flow\Parquet\Tests\Mother\ReadPlanMother;
use Flow\Parquet\Tests\Mother\SbbfMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\DSL\parquet_all;
use function Flow\Parquet\DSL\parquet_column;

final class ReadPlannerTest extends TestCase
{
    public function test_without_a_filter_the_window_is_read_and_matched_and_no_statistics_are_read(): void
    {
        $footer = new SpyFooter(Schema::with(FlatColumn::int64('id')), 10);
        $indexes = new InMemoryIndexes();

        static::assertEquals(
            ReadPlanMother::window(10, 2, 5),
            (new ReadPlanner())->plan($footer, $indexes, null, 2, 5),
        );
        static::assertSame([], $footer->rowGroupChunkReads);
        static::assertSame([], $indexes->reads);
    }

    public function test_a_limit_beside_a_wholly_bound_filter_counts_the_rows_the_row_filter_keeps(): void
    {
        $footer = FooterMother::idGroups([1, 2], [3, 4], [5, 6]);
        $filter = BoundFilter::bind(parquet_column('id')->greaterThan(1), $footer->schema(), new Options());

        $plan = (new ReadPlanner())->plan($footer, new InMemoryIndexes(), $filter, 1, 3);

        static::assertEquals(RowSelection::window(6, 1), $plan->selection);
        static::assertSame($filter, $plan->rowFilter);
        static::assertSame(3, $plan->limit);
    }

    public function test_a_filtered_plan_without_a_limit_carries_the_row_filter_and_no_limit(): void
    {
        $footer = FooterMother::idGroups([1, 2], [3, 4], [5, 6]);
        $kept = parquet_column('id')->equals(1);

        $plan = (new ReadPlanner())->plan(
            $footer,
            new InMemoryIndexes(),
            BoundFilter::bind(parquet_all($kept, parquet_column('id')->equals('1')), $footer->schema(), new Options()),
        );

        static::assertSame($kept, $plan->rowFilter?->filter());
        static::assertNull($plan->limit);
    }

    /**
     * @return Generator<string, array{Filter}>
     */
    public static function filters_not_wholly_bound(): Generator
    {
        yield 'partly bound' => [parquet_all(parquet_column('id')->equals(1), parquet_column('id')->equals('1'))];
        yield 'unbound' => [parquet_column('id')->equals('1')];
    }

    #[DataProvider('filters_not_wholly_bound')]
    public function test_a_limit_beside_a_filter_not_wholly_bound_is_refused(Filter $filter): void
    {
        $footer = new SpyFooter(Schema::with(FlatColumn::int64('id')), 10);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'A limit counts the rows returned, which a plan cannot honour unless every conjunct of its filter is bound '
            . 'in the file',
        );

        (new ReadPlanner())->plan(
            $footer,
            new InMemoryIndexes(),
            BoundFilter::bind($filter, $footer->schema(), new Options()),
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
                new InMemoryIndexes(),
                BoundFilter::bind(parquet_column('id')->equals(1), $footer->schema(), new Options()),
                limit: 0,
            ),
        );
        static::assertSame([], $footer->rowGroupChunkReads);
    }

    public function test_a_group_the_bloom_filter_drops_reads_no_page_index(): void
    {
        $footer = FooterMother::idGroups([0, 10], [0, 10]);
        $id = FlatColumn::int64('id');
        $indexes = new InMemoryIndexes(
            ['id' => [PageIndexMother::int64([[0, 10]]), PageIndexMother::int64([[0, 10]])]],
            ['id' => [PageIndexMother::pagesAt(0), PageIndexMother::pagesAt(0)]],
            ['id' => [SbbfMother::with($id, 0, 10), SbbfMother::with($id, 4)]],
        );

        static::assertEquals(
            new RowSelection(new RowSelector(2, true), new RowSelector(2, false)),
            (new ReadPlanner())->plan(
                $footer,
                $indexes,
                BoundFilter::bind(parquet_column('id')->equals(4), $footer->schema(), new Options()),
            )->selection,
        );
        static::assertSame(
            [
                ['bloomFilter', 'id', 0],
                ['bloomFilter', 'id', 1],
                ['columnIndex', 'id', 1],
                ['offsetIndex', 'id', 1],
            ],
            $indexes->reads,
        );
    }

    public function test_a_filter_asks_the_footer_for_the_bound_columns_only(): void
    {
        $id = FlatColumn::int64('id');
        $footer = new SpyFooter(Schema::with($id, FlatColumn::string('name')));

        (new ReadPlanner())->plan(
            $footer,
            new InMemoryIndexes(),
            BoundFilter::bind(
                parquet_all(parquet_column('id')->greaterThan(1), parquet_column('missing')->isNull()),
                $footer->schema(),
                new Options(),
            ),
        );

        static::assertEquals([[$id]], $footer->rowGroupChunkReads);
    }

    public function test_the_row_groups_statistics_narrow_the_window_from_its_offset_in_raw_rows(): void
    {
        $footer = FooterMother::idGroups([1, 2], [3, 4], [5, 6]);
        $filter = BoundFilter::bind(parquet_column('id')->greaterThan(2), $footer->schema(), new Options());
        $kept = new RowSelection(new RowSelector(2, true), new RowSelector(4, false));
        $fromThird = new RowSelection(new RowSelector(3, true), new RowSelector(3, false));
        $indexes = new InMemoryIndexes();

        static::assertEquals(
            new ReadPlan($kept, $kept, $indexes, $filter),
            (new ReadPlanner())->plan($footer, $indexes, $filter),
        );
        static::assertEquals(
            new ReadPlan($fromThird, $fromThird, $indexes, $filter),
            (new ReadPlanner())->plan($footer, $indexes, $filter, 3),
        );
    }

    public function test_the_page_index_narrows_the_row_groups_the_statistics_keep(): void
    {
        $footer = FooterMother::oneGroup(10, FlatColumn::int64('a'));
        $pages = new RowSelection(new RowSelector(5, true), new RowSelector(5, false));

        $indexes = new InMemoryIndexes(['a' => [PageIndexMother::int64([
            [0, 4],
            [5, 9],
        ])]], ['a' => [PageIndexMother::pagesAt(0, 5)]]);
        $filter = BoundFilter::bind(parquet_column('a')->greaterThanOrEqual(5), $footer->schema(), new Options());
        $plan = (new ReadPlanner())->plan($footer, $indexes, $filter);

        static::assertEquals(new ReadPlan($pages, $pages, $indexes, $filter), $plan);
        static::assertSame($indexes, $plan->indexes);
    }
}
