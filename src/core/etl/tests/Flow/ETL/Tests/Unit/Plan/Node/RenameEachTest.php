<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\RenameEach;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\Rename\RenameMapEntryStrategy;

final class RenameEachTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new RenameEach($input, [new RenameMapEntryStrategy(['id' => 'user_id'])]);

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $strategies = [new RenameMapEntryStrategy(['id' => 'user_id'])];
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
        $node = new RenameEach($input, [new RenameMapEntryStrategy(['id' => 'user_id'])]);

        static::assertSame(RowCount::preserving, $node->rowCount());
        static::assertSame(Transparency::transparent, $node->transparency());
        static::assertSame(Materialization::streaming, $node->materialization());
        static::assertEquals(Redefined::unknown(), $node->redefines());
    }

    public function test_no_strategies_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('At least one strategy must be provided.');

        new RenameEach(NodeMother::read(), []);
    }
}
