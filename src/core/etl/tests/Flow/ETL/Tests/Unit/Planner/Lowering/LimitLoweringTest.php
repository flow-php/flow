<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Planner\Lowering\LimitLowering;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\LimitTransformer;

use function array_map;

final class LimitLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [LimitTransformer::class],
            array_map(
                static fn($step) => $step::class,
                (new LimitLowering())->steps(new Limit(NodeMother::read(), 5), NodeMother::context(), []),
            ),
        );
    }
}
