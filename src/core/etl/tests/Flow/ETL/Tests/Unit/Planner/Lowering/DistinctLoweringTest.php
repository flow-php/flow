<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\Distinct;
use Flow\ETL\Planner\Lowering\DistinctLowering;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\DropDuplicatesTransformer;

use function array_map;

final class DistinctLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [DropDuplicatesTransformer::class],
            array_map(
                static fn($step) => $step::class,
                (new DistinctLowering())->steps(new Distinct(NodeMother::read(), ['id']), NodeMother::context(), []),
            ),
        );
    }
}
