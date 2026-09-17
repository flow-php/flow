<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\CrossJoin;
use Flow\ETL\Plan\Node\Outputs;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Sinks;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\to_memory;

final class CrossJoinTest extends FlowTestCase
{
    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $input = NodeMother::read();
        $frame = NodeMother::joinRight(NodeMother::plan(NodeMother::read()));
        $node = new CrossJoin($input, $frame, 'r_');

        static::assertSame($node, $node->withChildren([$input, $frame]));
    }

    public function test_with_children_returns_a_new_instance_when_a_child_changes(): void
    {
        $input = NodeMother::read();
        $other = NodeMother::read();
        $frame = NodeMother::joinRight(NodeMother::plan(NodeMother::read()));
        $node = new CrossJoin($input, $frame, 'r_');

        $rebuilt = $node->withChildren([$other, $frame]);

        static::assertNotSame($node, $rebuilt);
        static::assertInstanceOf(CrossJoin::class, $rebuilt);
        static::assertSame([$other, $frame], $rebuilt->children());
        static::assertSame('r_', $rebuilt->prefix);
    }

    public function test_with_children_returns_a_new_instance_when_the_right_side_changes(): void
    {
        $input = NodeMother::read();
        $frame = NodeMother::joinRight(NodeMother::plan(NodeMother::read()));
        $otherFrame = NodeMother::joinRight(NodeMother::plan(NodeMother::read()));
        $node = new CrossJoin($input, $frame, 'r_');

        $rebuilt = $node->withChildren([$input, $otherFrame]);

        static::assertNotSame($node, $rebuilt);
        static::assertSame([$input, $otherFrame], $rebuilt->children());
    }

    public function test_an_outputs_root_is_accepted_as_the_right_side(): void
    {
        $input = NodeMother::read();
        $read = NodeMother::read();
        $frame = new Outputs(new Result($read), new Sinks(new Write($read, to_memory(new ArrayMemory()))));

        static::assertSame([$input, $frame], (new CrossJoin($input, $frame, 'r_'))->children());
    }

    public function test_a_right_side_that_is_not_a_plan_root_is_refused(): void
    {
        $input = NodeMother::read();

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage(
            'The right side of a join must be a frame\'s plan root (Result or Outputs), ' . Read::class . ' given',
        );

        new CrossJoin($input, NodeMother::read(), 'r_');
    }

    public function test_with_children_refuses_a_right_side_that_is_not_a_plan_root(): void
    {
        $input = NodeMother::read();
        $frame = NodeMother::joinRight(NodeMother::plan(NodeMother::read()));
        $node = new CrossJoin($input, $frame, 'r_');

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage(
            'The right side of a join must be a frame\'s plan root (Result or Outputs), ' . Read::class . ' given',
        );

        $node->withChildren([$input, NodeMother::read()]);
    }

    public function test_declarations(): void
    {
        $input = NodeMother::read();
        $frame = NodeMother::joinRight(NodeMother::plan(NodeMother::read()));
        $node = new CrossJoin($input, $frame, 'r_');

        static::assertSame(RowCount::expanding, $node->rowCount());
        static::assertSame(Transparency::opaque, $node->transparency());
        static::assertSame(Materialization::streaming, $node->materialization());
        static::assertEquals(Redefined::unknown(), $node->redefines());
    }
}
