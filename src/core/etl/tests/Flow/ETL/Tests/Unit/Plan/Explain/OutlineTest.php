<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Explain;

use Flow\ETL\FlowContext;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Optimizer;
use Flow\ETL\Optimizer\Rule\CombineLimits;
use Flow\ETL\Plan;
use Flow\ETL\Plan\Explain\Outline;
use Flow\ETL\Plan\Node\CrossJoin;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Node\Outputs;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Plan\Sinks;
use Flow\ETL\Plan\Stage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\to_memory;

final class OutlineTest extends FlowTestCase
{
    public function test_a_node_is_numbered_after_everything_it_reads(): void
    {
        $read = NodeMother::read();
        $select = NodeMother::select($read);

        $root = (new Outline())->of($select);

        static::assertSame($select, $root->node);
        static::assertSame(2, $root->number);
        static::assertFalse($root->shared);
        static::assertCount(1, $root->children);
        static::assertSame($read, $root->children[0]->node);
        static::assertSame(1, $root->children[0]->number);
        static::assertSame([], $root->children[0]->children);
    }

    public function test_outputs_has_no_number_and_a_node_reached_again_is_a_shared_entry_with_its_number(): void
    {
        $select = NodeMother::select(NodeMother::read());
        $write = new Write($select, to_memory(new ArrayMemory()));

        $root = (new Outline())->of(new Outputs(new Result($select), new Sinks($write)));

        [$result, $sink] = $root->children;
        $shared = $sink->children[0];

        static::assertNull($root->number);
        static::assertSame(3, $result->number);
        static::assertSame(2, $result->children[0]->number);
        static::assertFalse($result->children[0]->shared);
        static::assertSame(4, $sink->number);
        static::assertSame($select, $shared->node);
        static::assertSame(2, $shared->number);
        static::assertTrue($shared->shared);
        static::assertSame([], $shared->children);
    }

    public function test_a_side_input_has_the_embedded_frames_plan_under_it(): void
    {
        $inner = NodeMother::plan(NodeMother::limit(NodeMother::read(), 5));
        $frame = NodeMother::frame($inner);

        $root = (new Outline())->of(new Result(new CrossJoin(NodeMother::read(), $frame)));

        $side = $root->children[0]->children[1];

        static::assertSame($frame, $side->node);
        static::assertCount(1, $side->children);
        static::assertSame($inner->root, $side->children[0]->node);
        static::assertSame(4, $side->children[0]->number);
        static::assertSame(5, $side->number);
    }

    public function test_the_optimized_stage_shows_an_embedded_frame_as_its_own_optimizer_rewrites_it(): void
    {
        $inner = NodeMother::plan(new Limit(new Limit(NodeMother::read(), 5), 3));
        $context = new FlowContext(config_builder()->optimizer(new Optimizer(new CombineLimits()))->build());
        $frame = NodeMother::frame($inner, $context);

        $limit = (new Outline(Stage::optimized))->of($frame)->children[0]->children[0];

        static::assertInstanceOf(Limit::class, $limit->node);
        static::assertSame(3, $limit->node->limit);
        static::assertInstanceOf(Read::class, $limit->children[0]->node);
    }

    public function test_logical_is_the_plan_at_the_outlines_stage(): void
    {
        $logical = NodeMother::plan(new Limit(new Limit(NodeMother::read(), 5), 3));
        $plan = Plan::of(
            $logical,
            new FlowContext(config_builder()->optimizer(new Optimizer(new CombineLimits()))->build()),
        );

        static::assertSame($logical, (new Outline())->logical($plan));
        static::assertNotSame($logical, (new Outline(Stage::optimized))->logical($plan));
    }

    public function test_a_node_both_frames_read_is_a_shared_entry_under_the_side_input(): void
    {
        $select = NodeMother::select(NodeMother::read());
        $frame = NodeMother::frame(NodeMother::plan($select));

        $root = (new Outline())->of(new Result(new CrossJoin($select, $frame)));

        $shared = $root->children[0]->children[1]->children[0]->children[0];

        static::assertSame($select, $shared->node);
        static::assertTrue($shared->shared);
        static::assertSame(2, $shared->number);
    }
}
