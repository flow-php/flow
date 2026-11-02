<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Optimizer\Rule;

use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Operators\GroupBy\DeclaredPivotValues;
use Flow\ETL\Operators\GroupBy\Pivot;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Aggregate;
use Flow\ETL\Planning\Logical\Node\Filter;
use Flow\ETL\Planning\Logical\Trigger;
use Flow\ETL\Tests\Context\ProjectionContext;
use Flow\ETL\Tests\Double\UnenumerableSum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Values\Sort\SortColumn;
use Flow\ETL\Values\SortOrder;

use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\sum;

final class PushProjectionIntoSourceTest extends FlowTestCase
{
    public function test_an_aggregate_pushes_its_group_and_aggregated_columns(): void
    {
        static::assertSame(
            ['country', 'amount', 'id'],
            ProjectionContext::pushed(
                new Aggregate(
                    NodeMother::ordersFileRead(),
                    new GroupBy(refs('country'), new Aggregators(sum(ref('amount')), count(ref('id')))),
                ),
            )->criteria()->columns,
        );
    }

    public function test_a_pivot_pushes_its_pivot_column_beside_the_group_and_aggregated_columns(): void
    {
        static::assertSame(
            ['country', 'amount', 'note'],
            ProjectionContext::pushed(
                new Aggregate(
                    NodeMother::ordersFileRead(),
                    new GroupBy(
                        refs('country'),
                        new Aggregators(sum(ref('amount'))),
                        new Pivot(ref('note'), new DeclaredPivotValues('a', 'b')),
                    ),
                ),
            )->criteria()->columns,
        );
    }

    public function test_an_aggregation_that_cannot_name_its_columns_pushes_none(): void
    {
        static::assertNull(ProjectionContext::pushed(
            new Aggregate(
                NodeMother::ordersFileRead(),
                new GroupBy(refs('country'), new Aggregators(new UnenumerableSum(ref('amount')))),
            ),
        )->criteria()->columns);
    }

    public function test_a_read_no_node_narrows_pushes_no_columns(): void
    {
        static::assertNull(ProjectionContext::pushed(
            new Filter(NodeMother::ordersFileRead(), ref('amount')->greaterThan(lit(1))),
        )->criteria()->columns);
    }

    public function test_a_select_above_a_filter_and_a_sort_pushes_what_each_of_them_reads(): void
    {
        static::assertSame(['country', 'note', 'amount'], ProjectionContext::pushed(NodeMother::select(NodeMother::sort(
            new Filter(NodeMother::ordersFileRead(), ref('amount')->greaterThan(lit(1))),
            [new SortColumn('note', SortOrder::DESC)],
        ), 'country'))->criteria()->columns);
    }

    public function test_a_conjunct_the_read_applies_itself_does_not_widen_its_columns(): void
    {
        static::assertSame(
            ['country'],
            ProjectionContext::pushed(NodeMother::select(
                NodeMother::ordersFileRead()->withFilter(ref('note')->isNotNull()),
                'country',
            ))->criteria()->columns,
        );
    }

    public function test_a_count_pushes_only_the_columns_the_nodes_below_it_read(): void
    {
        static::assertSame(
            ['amount'],
            ProjectionContext::pushed(
                new Filter(NodeMother::ordersFileRead(), ref('amount')->greaterThan(lit(1))),
                Trigger::count,
            )->criteria()->columns,
        );
    }

    public function test_a_count_over_a_bare_read_pushes_no_column(): void
    {
        static::assertSame(
            [],
            ProjectionContext::pushed(NodeMother::ordersFileRead(), Trigger::count)->criteria()->columns,
        );
    }

    public function test_a_read_that_is_no_file_source_is_left_alone(): void
    {
        static::assertNull(ProjectionContext::pushed(NodeMother::select(
            NodeMother::read(from_array([['id' => 1, 'country' => 'PL']])),
            'country',
        ))->criteria()->columns);
    }
}
