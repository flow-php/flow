<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Optimizer\Rule;

use Flow\ETL\Operators\Processor\RequiredColumns;
use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Outputs;
use Flow\ETL\Planning\Logical\Node\Result;
use Flow\ETL\Planning\Logical\Node\Write;
use Flow\ETL\Planning\Optimizer\Rule\PushProjectionIntoExpand;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\Context\ExpandColumnsContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure;
use function Flow\ETL\DSL\to_memory;

final class PushProjectionIntoExpandTest extends FlowTestCase
{
    public function test_a_select_above_an_expand_keeps_only_the_selected_columns(): void
    {
        static::assertEquals(
            ['record' => RequiredColumns::only('record')],
            ExpandColumnsContext::carries((new PushProjectionIntoExpand())->apply(
                NodeMother::plan(NodeMother::select(
                    new Node\ExpandColumn(NodeMother::read(), 'record', ref('body')->expand()),
                    'record',
                )),
                config()->backend(),
            )),
        );
    }

    public function test_a_drop_above_an_expand_keeps_every_column_but_the_dropped_ones(): void
    {
        static::assertEquals(
            ['record' => RequiredColumns::allBut('body')],
            ExpandColumnsContext::carries((new PushProjectionIntoExpand())->apply(
                NodeMother::plan(
                    new Node\Drop(new Node\ExpandColumn(NodeMother::read(), 'record', ref('body')->expand()), ['body']),
                ),
                config()->backend(),
            )),
        );
    }

    public function test_a_column_derived_between_the_expand_and_the_select_adds_what_it_reads(): void
    {
        static::assertEquals(
            ['record' => RequiredColumns::only('record', 'id')],
            ExpandColumnsContext::carries((new PushProjectionIntoExpand())->apply(
                NodeMother::plan(NodeMother::select(
                    new Node\WithColumn(
                        new Node\ExpandColumn(NodeMother::read(), 'record', ref('body')->expand()),
                        'y',
                        ref('id'),
                    ),
                    'record',
                    'y',
                )),
                config()->backend(),
            )),
        );
    }

    public function test_a_node_it_does_not_understand_leaves_the_plan_as_it_is(): void
    {
        $plan = NodeMother::plan(NodeMother::select(
            new Node\Rename(new Node\ExpandColumn(NodeMother::read(), 'record', ref('body')->expand()), 'record', 'r'),
            'r',
        ));

        static::assertSame($plan, (new PushProjectionIntoExpand())->apply($plan, config()->backend()));
    }

    public function test_an_expand_whose_rows_are_all_consumed_is_left_as_it_is(): void
    {
        $plan = NodeMother::plan(new Node\ExpandColumn(NodeMother::read(), 'record', ref('body')->expand()));

        static::assertSame($plan, (new PushProjectionIntoExpand())->apply($plan, config()->backend()));
    }

    public function test_stacked_expands_are_both_pruned(): void
    {
        static::assertEquals(
            ['b' => RequiredColumns::only('b'), 'a' => RequiredColumns::only('more')],
            ExpandColumnsContext::carries((new PushProjectionIntoExpand())->apply(
                NodeMother::plan(NodeMother::select(
                    new Node\ExpandColumn(
                        new Node\ExpandColumn(NodeMother::read(), 'a', ref('items')->expand()),
                        'b',
                        ref('more')->expand(),
                    ),
                    'b',
                )),
                config()->backend(),
            )),
        );
    }

    public function test_an_expand_shared_by_two_consumers_keeps_what_either_reads(): void
    {
        $expand = new Node\ExpandColumn(NodeMother::read(), 'record', ref('body')->expand());

        static::assertEquals(
            ['record' => RequiredColumns::only('record', 'id')],
            ExpandColumnsContext::carries((new PushProjectionIntoExpand())->apply(
                new LogicalPlan(
                    new Outputs(
                        new Result(NodeMother::select($expand, 'record')),
                        new Write(NodeMother::select($expand, 'record', 'id'), to_memory(new ArrayMemory())),
                    ),
                ),
                config()->backend(),
            )),
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

        static::assertSame($plan, (new PushProjectionIntoExpand())->apply($plan, config()->backend()));
    }

    public function test_a_select_requires_exactly_its_columns_whatever_is_above(): void
    {
        static::assertEquals(
            RequiredColumns::only('a', 'b'),
            (new PushProjectionIntoExpand())->below(new Node\Select(NodeMother::read(), [
                'a',
                ref('b'),
            ]), RequiredColumns::allBut('x')),
        );
    }

    public function test_a_drop_removes_its_columns_from_either_demand(): void
    {
        $drop = new Node\Drop(NodeMother::read(), ['body']);

        static::assertEquals(
            RequiredColumns::allBut('body'),
            (new PushProjectionIntoExpand())->below($drop, RequiredColumns::all()),
        );
        static::assertEquals(
            RequiredColumns::only('id'),
            (new PushProjectionIntoExpand())->below($drop, RequiredColumns::only('id', 'body')),
        );
    }

    public function test_a_with_column_replaces_its_own_name_with_what_it_reads(): void
    {
        $node = new Node\WithColumn(NodeMother::read(), 'y', concat(ref('a'), ref('b')));

        static::assertEquals(
            RequiredColumns::only('x', 'a', 'b'),
            (new PushProjectionIntoExpand())->below($node, RequiredColumns::only('x', 'y')),
        );
        static::assertEquals(
            RequiredColumns::allBut('z', 'y'),
            (new PushProjectionIntoExpand())->below($node, RequiredColumns::allBut('a', 'z')),
        );
    }

    public function test_an_expand_column_replaces_its_own_name_with_what_it_reads(): void
    {
        static::assertEquals(
            RequiredColumns::only('items'),
            (new PushProjectionIntoExpand())->below(
                new Node\ExpandColumn(NodeMother::read(), 'item', ref('items')->expand()),
                RequiredColumns::only('item'),
            ),
        );
    }

    public function test_an_unpack_keeps_the_demand_and_adds_what_it_reads(): void
    {
        static::assertEquals(
            RequiredColumns::only('s.a', 'items'),
            (new PushProjectionIntoExpand())->below(
                new Node\ExpandColumn(
                    NodeMother::read(),
                    's',
                    structure(['a' => ref('items')->expand()])->unpack(schema(int_schema('a'))),
                ),
                RequiredColumns::only('s.a'),
            ),
        );
        static::assertEquals(
            RequiredColumns::only('s', 'm'),
            (new PushProjectionIntoExpand())->below(
                new Node\WithColumn(NodeMother::read(), 's', ref('m')->unpack(schema(int_schema('a')))),
                RequiredColumns::only('s'),
            ),
        );
    }

    public function test_a_filter_adds_the_source_columns_its_predicate_reads(): void
    {
        static::assertEquals(
            RequiredColumns::only('x', 'id'),
            (new PushProjectionIntoExpand())->below(
                new Node\Filter(NodeMother::read(), ref('id')->as('alias')->isNotNull()),
                RequiredColumns::only('x'),
            ),
        );
    }

    public function test_a_limit_and_an_offset_pass_the_demand_through(): void
    {
        static::assertEquals(
            RequiredColumns::only('x'),
            (new PushProjectionIntoExpand())->below(
                NodeMother::limit(NodeMother::read(), 5),
                RequiredColumns::only('x'),
            ),
        );
        static::assertEquals(
            RequiredColumns::only('x'),
            (new PushProjectionIntoExpand())->below(new Node\Offset(NodeMother::read(), 5), RequiredColumns::only('x')),
        );
    }

    public function test_any_other_node_requires_every_column(): void
    {
        static::assertEquals(RequiredColumns::all(), (new PushProjectionIntoExpand())->below(
            new Node\Rename(NodeMother::read(), 'a', 'b'),
            RequiredColumns::only('b'),
        ));
    }
}
