<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Explain;

use Flow\ETL\Planning\Explain\LogicalOutline;
use Flow\ETL\Planning\Logical\Node\CrossJoin;
use Flow\ETL\Planning\Logical\Node\Outputs;
use Flow\ETL\Planning\Logical\Node\Result;
use Flow\ETL\Planning\Logical\Node\Write;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\to_memory;

final class LogicalOutlineTest extends FlowTestCase
{
    public function test_a_node_is_numbered_after_everything_it_reads(): void
    {
        $read = NodeMother::read();
        $select = NodeMother::select($read);

        $root = (new LogicalOutline())->of($select);

        static::assertSame($select, $root->source);
        static::assertSame(2, $root->number);
        static::assertFalse($root->shared);
        static::assertCount(1, $root->children);
        static::assertSame($read, $root->children[0]->source);
        static::assertSame(1, $root->children[0]->number);
        static::assertSame([], $root->children[0]->children);
    }

    public function test_outputs_has_no_number_and_a_node_reached_again_is_a_shared_entry_with_its_number(): void
    {
        $select = NodeMother::select(NodeMother::read());

        $root = (new LogicalOutline())->of(
            new Outputs(new Result($select), new Write($select, to_memory(new ArrayMemory()))),
        );

        [$result, $write] = $root->children;
        $shared = $write->children[0];

        static::assertNull($root->number);
        static::assertSame(3, $result->number);
        static::assertSame(2, $result->children[0]->number);
        static::assertFalse($result->children[0]->shared);
        static::assertSame(4, $write->number);
        static::assertSame($select, $shared->source);
        static::assertSame(2, $shared->number);
        static::assertTrue($shared->shared);
        static::assertSame([], $shared->children);
    }

    public function test_a_join_is_drawn_reading_what_its_right_sides_result_reads(): void
    {
        $limit = NodeMother::limit(NodeMother::read(), 5);

        $join = (new LogicalOutline())->of(new Result(
            new CrossJoin(NodeMother::read(), NodeMother::joinRight(NodeMother::plan($limit))),
        ))->children[0];
        $side = $join->children[1];

        static::assertSame($limit, $side->source);
        static::assertSame(3, $side->number);
        static::assertSame(2, $side->children[0]->number);
        static::assertSame(4, $join->number);
    }

    public function test_a_node_both_sides_read_is_a_shared_entry_as_the_right_side(): void
    {
        $select = NodeMother::select(NodeMother::read());

        $shared = (new LogicalOutline())->of(new Result(
            new CrossJoin($select, NodeMother::joinRight(NodeMother::plan($select))),
        ))->children[0]->children[1];

        static::assertSame($select, $shared->source);
        static::assertTrue($shared->shared);
        static::assertSame(2, $shared->number);
    }

    public function test_an_outputs_right_side_stays_in_the_tree(): void
    {
        $read = NodeMother::read();
        $frame = new Outputs(new Result($read), new Write($read, to_memory(new ArrayMemory())));

        $side = (new LogicalOutline())->of(new Result(
            new CrossJoin(NodeMother::read(), $frame),
        ))->children[0]->children[1];

        static::assertSame($frame, $side->source);
        static::assertNull($side->number);
    }
}
