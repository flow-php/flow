<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Constraint\UniqueConstraint;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\Constrain;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

final class ConstrainTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new Constrain($input, [new UniqueConstraint('id')]);

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $constraints = [new UniqueConstraint('id')];
        $node = new Constrain($input, $constraints);

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertInstanceOf(Constrain::class, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame($constraints, $rebuilt->constraints);
    }

    public function test_declarations(): void
    {
        $input = NodeMother::read();
        $node = new Constrain($input, [new UniqueConstraint('id')]);

        static::assertSame(RowCount::preserving, $node->rowCount());
        static::assertSame(Transparency::opaque, $node->transparency());
        static::assertSame(Materialization::streaming, $node->materialization());
        static::assertEquals(Redefined::none(), $node->redefines());
    }
}
