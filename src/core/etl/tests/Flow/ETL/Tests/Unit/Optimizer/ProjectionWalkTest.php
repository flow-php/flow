<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Optimizer;

use Flow\ETL\Optimizer\ProjectionWalk;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\RequiredColumns;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure;

final class ProjectionWalkTest extends FlowTestCase
{
    public function test_a_select_requires_exactly_its_columns_whatever_is_above(): void
    {
        static::assertEquals(
            RequiredColumns::only('a', 'b'),
            (new ProjectionWalk())->below(new Node\Select(NodeMother::read(), [
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
            (new ProjectionWalk())->below($drop, RequiredColumns::all()),
        );
        static::assertEquals(
            RequiredColumns::only('id'),
            (new ProjectionWalk())->below($drop, RequiredColumns::only('id', 'body')),
        );
    }

    public function test_a_with_column_replaces_its_own_name_with_what_it_reads(): void
    {
        $node = new Node\WithColumn(NodeMother::read(), 'y', concat(ref('a'), ref('b')));

        static::assertEquals(
            RequiredColumns::only('x', 'a', 'b'),
            (new ProjectionWalk())->below($node, RequiredColumns::only('x', 'y')),
        );
        static::assertEquals(
            RequiredColumns::allBut('z', 'y'),
            (new ProjectionWalk())->below($node, RequiredColumns::allBut('a', 'z')),
        );
    }

    public function test_an_expand_column_replaces_its_own_name_with_what_it_reads(): void
    {
        static::assertEquals(
            RequiredColumns::only('items'),
            (new ProjectionWalk())->below(
                new Node\ExpandColumn(NodeMother::read(), 'item', ref('items')->expand()),
                RequiredColumns::only('item'),
            ),
        );
    }

    public function test_an_unpack_keeps_the_demand_and_adds_what_it_reads(): void
    {
        static::assertEquals(
            RequiredColumns::only('s.a', 'items'),
            (new ProjectionWalk())->below(
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
            (new ProjectionWalk())->below(
                new Node\WithColumn(NodeMother::read(), 's', ref('m')->unpack(schema(int_schema('a')))),
                RequiredColumns::only('s'),
            ),
        );
    }

    public function test_a_filter_adds_the_source_columns_its_predicate_reads(): void
    {
        static::assertEquals(
            RequiredColumns::only('x', 'id'),
            (new ProjectionWalk())->below(
                new Node\Filter(NodeMother::read(), ref('id')->as('alias')->isNotNull()),
                RequiredColumns::only('x'),
            ),
        );
    }

    public function test_a_limit_and_an_offset_pass_the_demand_through(): void
    {
        static::assertEquals(
            RequiredColumns::only('x'),
            (new ProjectionWalk())->below(NodeMother::limit(NodeMother::read(), 5), RequiredColumns::only('x')),
        );
        static::assertEquals(
            RequiredColumns::only('x'),
            (new ProjectionWalk())->below(new Node\Offset(NodeMother::read(), 5), RequiredColumns::only('x')),
        );
    }

    public function test_any_other_node_requires_every_column(): void
    {
        static::assertEquals(RequiredColumns::all(), (new ProjectionWalk())->below(
            new Node\Rename(NodeMother::read(), 'a', 'b'),
            RequiredColumns::only('b'),
        ));
    }
}
