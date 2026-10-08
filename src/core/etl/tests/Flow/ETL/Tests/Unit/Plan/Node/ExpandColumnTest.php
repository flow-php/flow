<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\ExpandColumn;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RequiredColumns;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\array_expand;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure;

final class ExpandColumnTest extends FlowTestCase
{
    public function test_children_returns_its_input(): void
    {
        $input = NodeMother::read();

        static::assertSame([$input], (new ExpandColumn($input, 'item', array_expand(ref('items'))))->children());
    }

    public function test_carries_every_column_until_told_otherwise(): void
    {
        static::assertEquals(
            RequiredColumns::all(),
            (new ExpandColumn(NodeMother::read(), 'item', array_expand(ref('items'))))->carries,
        );
    }

    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new ExpandColumn($input, 'item', array_expand(ref('items')));

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_keeps_entry_function_and_carries(): void
    {
        $other = NodeMother::read();
        $function = array_expand(ref('items'));
        $rebuilt = (new ExpandColumn(
            NodeMother::read(),
            'item',
            $function,
            RequiredColumns::only('item'),
        ))->withChildren([$other]);

        static::assertSame([$other], $rebuilt->children());
        static::assertSame('item', $rebuilt->entry);
        static::assertSame($function, $rebuilt->function);
        static::assertEquals(RequiredColumns::only('item'), $rebuilt->carries);
    }

    public function test_with_carries_replaces_only_the_carries(): void
    {
        $input = NodeMother::read();
        $node = (new ExpandColumn($input, 'item', array_expand(ref('items'))))->withCarries(RequiredColumns::allBut(
            'items',
        ));

        static::assertEquals(RequiredColumns::allBut('items'), $node->carries);
        static::assertSame([$input], $node->children());
    }

    public function test_declarations(): void
    {
        $node = new ExpandColumn(NodeMother::read(), 'item', array_expand(ref('items')));

        static::assertSame(RowCount::expanding, $node->rowCount());
        static::assertSame(Transparency::transparent, $node->transparency());
        static::assertSame(Materialization::streaming, $node->materialization());
        static::assertEquals(Redefined::names('item'), $node->redefines());
    }

    public function test_an_expand_nested_in_a_structure_is_expanding_too(): void
    {
        static::assertSame(RowCount::expanding, (new ExpandColumn(NodeMother::read(), 'item', structure([
            'tag' => array_expand(ref('items')),
        ])))->rowCount());
    }

    public function test_redefines_the_definitions_name_when_the_entry_is_a_definition(): void
    {
        $node = new ExpandColumn(NodeMother::read(), str_schema('item'), array_expand(ref('items')));

        static::assertSame('item', $node->name());
        static::assertEquals(Redefined::names('item'), $node->redefines());
    }
}
