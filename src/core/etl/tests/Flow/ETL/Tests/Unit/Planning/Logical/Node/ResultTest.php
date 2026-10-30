<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Logical\Node;

use Flow\ETL\Planning\Logical\Node\Result;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

final class ResultTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new Result($input);

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_the_input_changes(): void
    {
        $node = new Result(NodeMother::read());
        $other = NodeMother::read();

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
    }

    public function test_declarations(): void
    {
        $node = new Result(NodeMother::read());

        static::assertSame(RowCount::preserving, $node->rowCount());
        static::assertSame(Transparency::transparent, $node->transparency());
        static::assertEquals(Redefined::none(), $node->redefines());
    }
}
