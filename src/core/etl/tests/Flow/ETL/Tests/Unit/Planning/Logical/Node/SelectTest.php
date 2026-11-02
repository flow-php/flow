<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Logical\Node;

use Flow\ETL\Operators\Processor\RequiredColumns;
use Flow\ETL\Planning\Logical\Node\Select;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\ref;

final class SelectTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new Select($input, ['id']);

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $node = new Select($input, ['id']);

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertInstanceOf(Select::class, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame(['id'], $rebuilt->columns);
    }

    public function test_declarations(): void
    {
        $input = NodeMother::read();
        $node = new Select($input, ['id']);

        static::assertSame(RowCount::preserving, $node->rowCount());
        static::assertSame(Transparency::transparent, $node->transparency());
        static::assertEquals(Redefined::allBut('id'), $node->redefines());
    }

    public function test_a_select_requires_exactly_its_columns_whatever_is_above(): void
    {
        static::assertEquals(
            RequiredColumns::only('a', 'b'),
            (new Select(NodeMother::read(), [
                'a',
                ref('b'),
            ]))->requiredInputColumns(RequiredColumns::allBut('x')),
        );
    }
}
