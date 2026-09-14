<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\Transform;
use Flow\ETL\Planner\Lowering\TransformLowering;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\SelectEntriesTransformer;

final class TransformLoweringTest extends FlowTestCase
{
    public function test_the_instance_the_node_holds_is_the_step(): void
    {
        $transformer = new SelectEntriesTransformer('id');

        static::assertSame(
            [$transformer],
            (new TransformLowering())->steps(
                new Transform(NodeMother::read(), $transformer),
                NodeMother::context(),
                [],
            ),
        );
    }
}
