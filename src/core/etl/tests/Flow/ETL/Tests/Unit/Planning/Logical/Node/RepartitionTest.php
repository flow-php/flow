<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Logical\Node;

use Flow\ETL\Planning\Logical\Node\Repartition;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\refs;

final class RepartitionTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new Repartition($input, refs('id'));

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $by = refs('id');
        $node = new Repartition($input, $by);

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertInstanceOf(Repartition::class, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame($by, $rebuilt->by);
    }

    public function test_declarations(): void
    {
        $input = NodeMother::read();
        $node = new Repartition($input, refs('id'));

        static::assertSame(RowCount::preserving, $node->rowCount());
        static::assertSame(Transparency::opaque, $node->transparency());
        static::assertEquals(Redefined::none(), $node->redefines());
    }
}
