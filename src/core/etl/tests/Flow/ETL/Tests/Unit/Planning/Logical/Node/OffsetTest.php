<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Logical\Node;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Planning\Logical\Node\Offset;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

final class OffsetTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new Offset($input, 3);

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $node = new Offset($input, 3);

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertInstanceOf(Offset::class, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame(3, $rebuilt->offset);
    }

    public function test_declarations(): void
    {
        $input = NodeMother::read();
        $node = new Offset($input, 3);

        static::assertSame(RowCount::reducing, $node->rowCount());
        static::assertSame(Transparency::transparent, $node->transparency());
        static::assertEquals(Redefined::none(), $node->redefines());
    }

    public function test_a_negative_offset_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Offset must be greater than or equal to 0, given: -1');

        // @mago-ignore analysis:invalid-argument
        new Offset(NodeMother::read(), -1);
    }
}
