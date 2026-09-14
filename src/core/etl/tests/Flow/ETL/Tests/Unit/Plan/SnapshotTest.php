<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan;

use Flow\ETL\Plan\Snapshot;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

final class SnapshotTest extends FlowTestCase
{
    public function test_plan_and_context_are_the_values_it_was_built_with(): void
    {
        $plan = NodeMother::plan(NodeMother::read());
        $context = NodeMother::context();

        $snapshot = new Snapshot($plan, $context);

        static::assertSame($plan, $snapshot->plan);
        static::assertSame($context, $snapshot->context);
    }

    public function test_with_root_returns_the_same_snapshot_when_the_root_is_identical(): void
    {
        $root = NodeMother::read();
        $snapshot = new Snapshot(NodeMother::plan($root), NodeMother::context());

        static::assertSame($snapshot, $snapshot->withRoot($snapshot->plan->root));
    }

    public function test_with_root_keeps_the_context(): void
    {
        $context = NodeMother::context();
        $snapshot = new Snapshot(NodeMother::plan(NodeMother::read()), $context);
        $root = NodeMother::select(NodeMother::read());

        $rebuilt = $snapshot->withRoot($root);

        static::assertNotSame($snapshot, $rebuilt);
        static::assertSame($root, $rebuilt->plan->root);
        static::assertSame($context, $rebuilt->context);
    }
}
