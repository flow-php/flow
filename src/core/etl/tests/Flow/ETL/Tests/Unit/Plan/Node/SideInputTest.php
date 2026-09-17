<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\SideInput;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

final class SideInputTest extends FlowTestCase
{
    public function test_a_frame_is_a_leaf(): void
    {
        static::assertSame([], NodeMother::frame(NodeMother::plan(NodeMother::select(NodeMother::read())))->children());
    }

    public function test_with_children_returns_the_same_instance(): void
    {
        $frame = NodeMother::frame(NodeMother::plan(NodeMother::read()));

        static::assertSame($frame, $frame->withChildren([]));
    }

    public function test_plan_is_the_one_it_was_built_with(): void
    {
        $plan = NodeMother::framePlan(NodeMother::plan(NodeMother::select(NodeMother::read())));

        static::assertSame($plan, (new SideInput($plan))->plan());
    }

    public function test_declarations(): void
    {
        $frame = NodeMother::frame(NodeMother::plan(NodeMother::read()));

        static::assertSame(RowCount::preserving, $frame->rowCount());
        static::assertSame(Transparency::opaque, $frame->transparency());
        static::assertSame(Materialization::streaming, $frame->materialization());
        static::assertEquals(Redefined::none(), $frame->redefines());
    }
}
