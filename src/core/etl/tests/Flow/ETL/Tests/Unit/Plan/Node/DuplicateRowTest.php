<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\DuplicateRow;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\WithEntry;

use function Flow\ETL\DSL\lit;

final class DuplicateRowTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new DuplicateRow($input, lit(true), [new WithEntry('copy', lit(1))]);

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $condition = lit(true);
        $entries = [new WithEntry('copy', lit(1))];
        $node = new DuplicateRow($input, $condition, $entries);

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertInstanceOf(DuplicateRow::class, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame($condition, $rebuilt->condition);
        static::assertSame($entries, $rebuilt->entries);
    }

    public function test_declarations(): void
    {
        $input = NodeMother::read();
        $node = new DuplicateRow($input, lit(true), [new WithEntry('copy', lit(1))]);

        static::assertSame(RowCount::expanding, $node->rowCount());
        static::assertSame(Transparency::transparent, $node->transparency());
        static::assertSame(Materialization::streaming, $node->materialization());
        static::assertEquals(Redefined::names('copy'), $node->redefines());
    }
}
