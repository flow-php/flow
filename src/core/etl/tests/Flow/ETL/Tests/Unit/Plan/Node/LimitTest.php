<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

final class LimitTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new Limit($input, 5);

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $node = new Limit($input, 5);

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertInstanceOf(Limit::class, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame(5, $rebuilt->limit);
    }

    public function test_declarations(): void
    {
        $input = NodeMother::read();
        $node = new Limit($input, 5);

        static::assertSame(RowCount::reducing, $node->rowCount());
        static::assertSame(Transparency::transparent, $node->transparency());
        static::assertSame(Materialization::streaming, $node->materialization());
        static::assertEquals(Redefined::none(), $node->redefines());
    }

    public function test_a_limit_of_zero_or_less_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Limit can\'t be lower or equal zero, given: 0');

        new Limit(NodeMother::read(), 0);
    }
}
