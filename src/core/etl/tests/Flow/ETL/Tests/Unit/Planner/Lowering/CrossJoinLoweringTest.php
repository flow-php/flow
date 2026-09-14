<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\CrossJoin;
use Flow\ETL\Planner\Lowering\CrossJoinLowering;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\FrameOutputMother;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\CrossJoinRowsTransformer;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class CrossJoinLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        $frame = NodeMother::frame(NodeMother::plan(NodeMother::read()));
        $right = FrameOutputMother::reading(from_array([['id' => 1]], schema(int_schema('id'))));

        static::assertEquals(
            [new CrossJoinRowsTransformer($right, 'r_')],
            (new CrossJoinLowering())->steps(
                new CrossJoin(NodeMother::read(), $frame, 'r_'),
                NodeMother::context(),
                [$right],
            ),
        );
    }
}
