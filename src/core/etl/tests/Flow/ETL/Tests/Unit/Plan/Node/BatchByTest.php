<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\BatchBy;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\ref;

final class BatchByTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new BatchBy($input, ref('id'), 5);

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $column = ref('id');
        $node = new BatchBy($input, $column, 5);

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertInstanceOf(BatchBy::class, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame($column, $rebuilt->column);
        static::assertSame(5, $rebuilt->minSize);
    }

    public function test_declarations(): void
    {
        $input = NodeMother::read();
        $node = new BatchBy($input, ref('id'), 5);

        static::assertSame(RowCount::preserving, $node->rowCount());
        static::assertSame(Transparency::transparent, $node->transparency());
        static::assertSame(Materialization::streaming, $node->materialization());
        static::assertEquals(Redefined::none(), $node->redefines());
    }

    public function test_a_min_size_of_zero_or_less_is_refused(): void
    {
        $column = ref('id');
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Minimum batch size must be greater than 0, given: 0');

        // @mago-ignore analysis:invalid-argument
        new BatchBy(NodeMother::read(), $column, 0);
    }
}
