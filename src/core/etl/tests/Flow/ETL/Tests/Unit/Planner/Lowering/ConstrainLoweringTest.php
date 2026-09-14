<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Constraint\UniqueConstraint;
use Flow\ETL\Plan\Node\Constrain;
use Flow\ETL\Planner\Lowering\ConstrainLowering;
use Flow\ETL\Processor\ConstrainedProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function array_map;

final class ConstrainLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [ConstrainedProcessor::class],
            array_map(
                static fn($step) => $step::class,
                (new ConstrainLowering())->steps(
                    new Constrain(NodeMother::read(), [new UniqueConstraint('id')]),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }
}
