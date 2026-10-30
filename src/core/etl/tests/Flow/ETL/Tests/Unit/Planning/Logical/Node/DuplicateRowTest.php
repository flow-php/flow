<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Logical\Node;

use Flow\ETL\Functions\WithColumn;
use Flow\ETL\Planning\Logical\Node\DuplicateRow;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\lit;

final class DuplicateRowTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new DuplicateRow($input, lit(true), [new WithColumn('copy', lit(1))]);

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $condition = lit(true);
        $columns = [new WithColumn('copy', lit(1))];
        $node = new DuplicateRow($input, $condition, $columns);

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertInstanceOf(DuplicateRow::class, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame($condition, $rebuilt->condition);
        static::assertSame($columns, $rebuilt->columns);
    }

    public function test_declarations(): void
    {
        $node = new DuplicateRow(NodeMother::read(), lit(true), [new WithColumn('copy', lit(1))]);

        static::assertSame(RowCount::expanding, $node->rowCount());
        static::assertSame(Transparency::transparent, $node->transparency());
        static::assertEquals(Redefined::names('copy'), $node->redefines());
    }
}
