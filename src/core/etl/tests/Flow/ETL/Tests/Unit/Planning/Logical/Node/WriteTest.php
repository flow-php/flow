<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Logical\Node;

use Flow\ETL\Planning\Logical\Node\Write;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\to_memory;

final class WriteTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new Write($input, to_memory(new ArrayMemory()));

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $loader = to_memory(new ArrayMemory());
        $node = new Write($input, $loader);

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertInstanceOf(Write::class, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame($loader, $rebuilt->loader);
    }

    public function test_declarations(): void
    {
        $node = new Write(NodeMother::read(), to_memory(new ArrayMemory()));

        static::assertSame(RowCount::preserving, $node->rowCount());
        static::assertSame(Transparency::opaque, $node->transparency());
        static::assertEquals(Redefined::none(), $node->redefines());
    }
}
