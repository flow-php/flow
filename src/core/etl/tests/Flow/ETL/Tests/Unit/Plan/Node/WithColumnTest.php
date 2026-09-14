<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\WithColumn;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\array_expand;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\structure;

final class WithColumnTest extends FlowTestCase
{
    public function test_children_returns_its_inputs(): void
    {
        $input = NodeMother::read();

        static::assertSame([$input], (new WithColumn($input, 'doubled', ref('id')->multiply(lit(2))))->children());
    }

    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new WithColumn($input, 'doubled', ref('id')->multiply(lit(2)));

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $other = NodeMother::read();
        $function = ref('id')->multiply(lit(2));
        $node = new WithColumn(NodeMother::read(), 'doubled', $function);

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame('doubled', $rebuilt->entry);
        static::assertSame($function, $rebuilt->function);
    }

    public function test_entry_and_function_are_the_values_it_was_built_with(): void
    {
        $function = ref('id')->multiply(lit(2));
        $node = new WithColumn(NodeMother::read(), 'doubled', $function);

        static::assertSame('doubled', $node->entry);
        static::assertSame($function, $node->function);
    }

    public function test_declarations(): void
    {
        $node = new WithColumn(NodeMother::read(), 'doubled', ref('id')->multiply(lit(2)));

        static::assertSame(RowCount::preserving, $node->rowCount());
        static::assertSame(Transparency::transparent, $node->transparency());
        static::assertSame(Materialization::streaming, $node->materialization());
        static::assertEquals(Redefined::names('doubled'), $node->redefines());
    }

    public function test_row_count_is_expanding_when_the_function_tree_contains_array_expand(): void
    {
        static::assertSame(
            RowCount::expanding,
            (new WithColumn(NodeMother::read(), 'item', array_expand(ref('items'))))->rowCount(),
        );
        static::assertSame(RowCount::expanding, (new WithColumn(NodeMother::read(), 'item', structure([
            'tag' => array_expand(ref('items')),
        ])))->rowCount());
    }

    public function test_redefines_the_definitions_name_when_the_entry_is_a_definition(): void
    {
        $node = new WithColumn(NodeMother::read(), int_schema('doubled'), ref('id')->multiply(lit(2)));

        static::assertEquals(Redefined::names('doubled'), $node->redefines());
    }
}
