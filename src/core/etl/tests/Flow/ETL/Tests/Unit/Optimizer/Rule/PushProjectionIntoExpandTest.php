<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Optimizer\Rule;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Optimizer\Rule\PushProjectionIntoExpand;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Outputs;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Plan\RequiredColumns;
use Flow\ETL\Tests\Context\ExpandColumnsContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;

final class PushProjectionIntoExpandTest extends FlowTestCase
{
    public function test_a_select_above_an_expand_keeps_only_the_selected_columns(): void
    {
        $plan = NodeMother::plan(NodeMother::select(
            new Node\ExpandColumn(NodeMother::read(), 'record', ref('body')->expand()),
            'record',
        ));

        static::assertEquals(
            ['record' => RequiredColumns::only('record')],
            ExpandColumnsContext::carries((new PushProjectionIntoExpand())->apply($plan, NodeMother::context())),
        );
    }

    public function test_a_drop_above_an_expand_keeps_every_column_but_the_dropped_ones(): void
    {
        $plan = NodeMother::plan(
            new Node\Drop(new Node\ExpandColumn(NodeMother::read(), 'record', ref('body')->expand()), ['body']),
        );

        static::assertEquals(
            ['record' => RequiredColumns::allBut('body')],
            ExpandColumnsContext::carries((new PushProjectionIntoExpand())->apply($plan, NodeMother::context())),
        );
    }

    public function test_a_column_derived_between_the_expand_and_the_select_adds_what_it_reads(): void
    {
        $plan = NodeMother::plan(NodeMother::select(
            new Node\WithColumn(
                new Node\ExpandColumn(NodeMother::read(), 'record', ref('body')->expand()),
                'y',
                ref('id'),
            ),
            'record',
            'y',
        ));

        static::assertEquals(
            ['record' => RequiredColumns::only('record', 'id')],
            ExpandColumnsContext::carries((new PushProjectionIntoExpand())->apply($plan, NodeMother::context())),
        );
    }

    public function test_a_node_it_does_not_understand_leaves_the_plan_as_it_is(): void
    {
        $plan = NodeMother::plan(NodeMother::select(
            new Node\Rename(new Node\ExpandColumn(NodeMother::read(), 'record', ref('body')->expand()), 'record', 'r'),
            'r',
        ));

        static::assertSame($plan, (new PushProjectionIntoExpand())->apply($plan, NodeMother::context()));
    }

    public function test_an_expand_whose_rows_are_all_consumed_is_left_as_it_is(): void
    {
        $plan = NodeMother::plan(new Node\ExpandColumn(NodeMother::read(), 'record', ref('body')->expand()));

        static::assertSame($plan, (new PushProjectionIntoExpand())->apply($plan, NodeMother::context()));
    }

    public function test_stacked_expands_are_both_pruned(): void
    {
        $plan = NodeMother::plan(NodeMother::select(
            new Node\ExpandColumn(
                new Node\ExpandColumn(NodeMother::read(), 'a', ref('items')->expand()),
                'b',
                ref('more')->expand(),
            ),
            'b',
        ));

        static::assertEquals(
            ['b' => RequiredColumns::only('b'), 'a' => RequiredColumns::only('more')],
            ExpandColumnsContext::carries((new PushProjectionIntoExpand())->apply($plan, NodeMother::context())),
        );
    }

    public function test_an_expand_shared_by_two_consumers_keeps_what_either_reads(): void
    {
        $expand = new Node\ExpandColumn(NodeMother::read(), 'record', ref('body')->expand());
        $plan = new LogicalPlan(
            new Outputs(
                new Result(NodeMother::select($expand, 'record')),
                new Write(NodeMother::select($expand, 'record', 'id'), to_memory(new ArrayMemory())),
            ),
        );

        static::assertEquals(
            ['record' => RequiredColumns::only('record', 'id')],
            ExpandColumnsContext::carries((new PushProjectionIntoExpand())->apply($plan, NodeMother::context())),
        );
    }

    public function test_a_consumer_reading_every_column_keeps_the_shared_expand_as_it_is(): void
    {
        $expand = new Node\ExpandColumn(NodeMother::read(), 'record', ref('body')->expand());
        $plan = new LogicalPlan(
            new Outputs(
                new Result(NodeMother::select($expand, 'record')),
                new Write($expand, to_memory(new ArrayMemory())),
            ),
        );

        static::assertSame($plan, (new PushProjectionIntoExpand())->apply($plan, NodeMother::context()));
    }
}
