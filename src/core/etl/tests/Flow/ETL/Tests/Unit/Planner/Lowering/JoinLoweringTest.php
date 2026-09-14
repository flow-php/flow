<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Join\Join as JoinType;
use Flow\ETL\Plan\Node\Join;
use Flow\ETL\Planner\Lowering\JoinLowering;
use Flow\ETL\Processor\HashJoinProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\FrameOutputMother;
use Flow\ETL\Tests\Mother\NodeMother;

use function array_map;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\hash_join;
use function Flow\ETL\DSL\join_on;

final class JoinLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        $frame = NodeMother::frame(NodeMother::plan(NodeMother::read()));

        static::assertSame(
            [HashJoinProcessor::class],
            array_map(static fn($step) => $step::class, (new JoinLowering())->steps(
                new Join(NodeMother::read(), $frame, join_on(['id' => 'id']), JoinType::inner, hash_join()),
                NodeMother::context(),
                [FrameOutputMother::reading(from_array([['id' => 1]]))],
            )),
        );
    }
}
