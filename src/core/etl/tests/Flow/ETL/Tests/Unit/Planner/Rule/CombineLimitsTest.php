<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Rule;

use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Node\Select;
use Flow\ETL\Planner\Rule\CombineLimits;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

final class CombineLimitsTest extends FlowTestCase
{
    public function test_two_adjacent_limits_fold_to_the_minimum(): void
    {
        $read = NodeMother::read();
        $plan = NodeMother::plan(NodeMother::limit(NodeMother::limit($read, 10), 3));

        $root = (new CombineLimits())->apply($plan, NodeMother::context())->root->children()[0];

        static::assertInstanceOf(Limit::class, $root);
        static::assertSame(3, $root->limit);
        static::assertSame([$read], $root->children());
    }

    public function test_three_adjacent_limits_fold_to_the_minimum(): void
    {
        $read = NodeMother::read();
        $plan = NodeMother::plan(NodeMother::limit(NodeMother::limit(NodeMother::limit($read, 10), 2), 7));

        $root = (new CombineLimits())->apply($plan, NodeMother::context())->root->children()[0];

        static::assertInstanceOf(Limit::class, $root);
        static::assertSame(2, $root->limit);
        static::assertSame([$read], $root->children());
    }

    public function test_non_adjacent_limits_are_left_alone(): void
    {
        $plan = NodeMother::plan(NodeMother::limit(NodeMother::select(NodeMother::limit(NodeMother::read(), 10)), 3));

        $root = (new CombineLimits())->apply($plan, NodeMother::context())->root->children()[0];

        static::assertSame($plan->root->children()[0], $root);
        static::assertInstanceOf(Limit::class, $root);
        static::assertSame(3, $root->limit);
        static::assertInstanceOf(Select::class, $root->children()[0]);
        static::assertInstanceOf(Limit::class, $root->children()[0]->children()[0]);
    }
}
