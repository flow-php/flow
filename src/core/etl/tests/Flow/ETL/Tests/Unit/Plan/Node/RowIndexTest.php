<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\RowIndex;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;

final class RowIndexTest extends FlowTestCase
{
    public function test_children_is_the_input(): void
    {
        $input = NodeMother::read();

        static::assertSame([$input], (new RowIndex($input, 'idx', StartFrom::ZERO))->children());
    }

    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new RowIndex($input, 'idx', StartFrom::ZERO);

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_rebuilds_and_keeps_the_column_and_the_start(): void
    {
        $node = new RowIndex(NodeMother::read(), 'idx', StartFrom::ONE);
        $other = NodeMother::select(NodeMother::read());

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame('idx', $rebuilt->indexColumn);
        static::assertSame(StartFrom::ONE, $rebuilt->startFrom);
    }

    public function test_declarations(): void
    {
        $node = new RowIndex(NodeMother::read(), 'idx', StartFrom::ZERO);

        static::assertSame(RowCount::preserving, $node->rowCount());
        static::assertSame(Transparency::opaque, $node->transparency());
        static::assertSame(Materialization::streaming, $node->materialization());
        static::assertEquals(Redefined::names('idx'), $node->redefines());
    }
}
