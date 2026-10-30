<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Logical\Node;

use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Planning\Logical\Node\Join;
use Flow\ETL\Planning\Logical\Node\Outputs;
use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Logical\Node\Result;
use Flow\ETL\Planning\Logical\Node\Write;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Values\Join\JoinType;

use function Flow\ETL\DSL\hash_join;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\to_memory;

final class JoinTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $frame = NodeMother::joinRight(NodeMother::plan(NodeMother::read()));
        $node = new Join($input, $frame, join_on(['id' => 'id']), JoinType::inner, hash_join());

        static::assertSame($node, $node->withChildren([$input, $frame]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $frame = NodeMother::joinRight(NodeMother::plan(NodeMother::read()));
        $on = join_on(['id' => 'id']);
        $algorithm = hash_join();
        $node = new Join($input, $frame, $on, JoinType::inner, $algorithm);

        $rebuilt = $node->withChildren([$other, $frame]);

        static::assertNotSame($node, $rebuilt);
        static::assertInstanceOf(Join::class, $rebuilt);
        static::assertSame([$other, $frame], $rebuilt->children());
        static::assertSame($on, $rebuilt->on);
        static::assertSame(JoinType::inner, $rebuilt->type);
        static::assertSame($algorithm, $rebuilt->algorithm);
    }

    public function test_with_children_returns_a_new_instance_when_the_right_side_changes(): void
    {
        $input = NodeMother::read();
        $frame = NodeMother::joinRight(NodeMother::plan(NodeMother::read()));
        $otherFrame = NodeMother::joinRight(NodeMother::plan(NodeMother::read()));
        $node = new Join($input, $frame, join_on(['id' => 'id']), JoinType::inner, hash_join());

        $rebuilt = $node->withChildren([$input, $otherFrame]);

        static::assertNotSame($node, $rebuilt);
        static::assertSame([$input, $otherFrame], $rebuilt->children());
    }

    public function test_an_outputs_root_is_accepted_as_the_right_side(): void
    {
        $input = NodeMother::read();
        $read = NodeMother::read();
        $frame = new Outputs(new Result($read), new Write($read, to_memory(new ArrayMemory())));

        static::assertSame(
            [$input, $frame],
            (new Join($input, $frame, join_on(['id' => 'id']), JoinType::inner, hash_join()))->children(),
        );
    }

    public function test_a_right_side_that_is_not_a_plan_root_is_refused(): void
    {
        $input = NodeMother::read();

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage(
            'The right side of a join must be a frame\'s plan root (Result or Outputs), ' . Read::class . ' given',
        );

        new Join($input, NodeMother::read(), join_on(['id' => 'id']), JoinType::inner, hash_join());
    }

    public function test_with_children_refuses_a_right_side_that_is_not_a_plan_root(): void
    {
        $input = NodeMother::read();
        $node = new Join(
            $input,
            NodeMother::joinRight(NodeMother::plan(NodeMother::read())),
            join_on(['id' => 'id']),
            JoinType::inner,
            hash_join(),
        );

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage(
            'The right side of a join must be a frame\'s plan root (Result or Outputs), ' . Read::class . ' given',
        );

        $node->withChildren([$input, NodeMother::read()]);
    }

    public function test_declarations(): void
    {
        $node = new Join(
            NodeMother::read(),
            NodeMother::joinRight(NodeMother::plan(NodeMother::read())),
            join_on(['id' => 'id']),
            JoinType::inner,
            hash_join(),
        );

        static::assertSame(RowCount::unknown, $node->rowCount());
        static::assertSame(Transparency::opaque, $node->transparency());
        static::assertEquals(Redefined::unknown(), $node->redefines());
    }
}
