<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Logical\Node;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Operators\Transformer\Rename\RenameMapColumnStrategy;
use Flow\ETL\Planning\Logical\Node\RenameEach;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

final class RenameEachTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new RenameEach($input, [new RenameMapColumnStrategy(['id' => 'user_id'])]);

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $strategies = [new RenameMapColumnStrategy(['id' => 'user_id'])];
        $node = new RenameEach($input, $strategies);

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertInstanceOf(RenameEach::class, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame($strategies, $rebuilt->strategies);
    }

    public function test_declarations(): void
    {
        $input = NodeMother::read();
        $node = new RenameEach($input, [new RenameMapColumnStrategy(['id' => 'user_id'])]);

        static::assertSame(RowCount::preserving, $node->rowCount());
        static::assertSame(Transparency::transparent, $node->transparency());
        static::assertEquals(Redefined::unknown(), $node->redefines());
    }

    public function test_no_strategies_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('At least one strategy must be provided.');

        new RenameEach(NodeMother::read(), []);
    }
}
