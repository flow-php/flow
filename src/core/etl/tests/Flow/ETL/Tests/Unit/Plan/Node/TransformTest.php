<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\Transform;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\SelectEntriesTransformer;

final class TransformTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new Transform($input, new SelectEntriesTransformer('id'));

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $transformer = new SelectEntriesTransformer('id');
        $node = new Transform($input, $transformer);

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertInstanceOf(Transform::class, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame($transformer, $rebuilt->transformer);
    }

    public function test_declarations(): void
    {
        $input = NodeMother::read();
        $node = new Transform($input, new SelectEntriesTransformer('id'));

        static::assertSame(RowCount::unknown, $node->rowCount());
        static::assertSame(Transparency::opaque, $node->transparency());
        static::assertSame(Materialization::streaming, $node->materialization());
        static::assertEquals(Redefined::unknown(), $node->redefines());
    }
}
