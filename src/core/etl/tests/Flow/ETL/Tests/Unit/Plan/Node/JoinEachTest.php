<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Join\Join as JoinType;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\JoinEach;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Tests\Double\StaticDataFrameFactory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\join_on;

final class JoinEachTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $node = new JoinEach(
            $input,
            new StaticDataFrameFactory(df()->read(from_array([['id' => 1]]))),
            join_on(['id' => 'id']),
            JoinType::left,
        );

        static::assertSame($node, $node->withChildren([$input]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $factory = new StaticDataFrameFactory(df()->read(from_array([['id' => 1]])));
        $on = join_on(['id' => 'id']);
        $node = new JoinEach($input, $factory, $on, JoinType::left);

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertInstanceOf(JoinEach::class, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame($factory, $rebuilt->factory);
        static::assertSame($on, $rebuilt->on);
        static::assertSame(JoinType::left, $rebuilt->type);
    }

    public function test_declarations(): void
    {
        $input = NodeMother::read();
        $node = new JoinEach(
            $input,
            new StaticDataFrameFactory(df()->read(from_array([['id' => 1]]))),
            join_on(['id' => 'id']),
            JoinType::left,
        );

        static::assertSame(RowCount::unknown, $node->rowCount());
        static::assertSame(Transparency::opaque, $node->transparency());
        static::assertSame(Materialization::streaming, $node->materialization());
        static::assertEquals(Redefined::unknown(), $node->redefines());
    }
}
