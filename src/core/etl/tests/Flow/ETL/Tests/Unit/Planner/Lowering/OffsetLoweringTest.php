<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\Offset;
use Flow\ETL\Planner\Lowering\OffsetLowering;
use Flow\ETL\Processor\OffsetProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function array_map;

final class OffsetLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [OffsetProcessor::class],
            array_map(
                static fn($step) => $step::class,
                (new OffsetLowering())->steps(new Offset(NodeMother::read(), 3), NodeMother::context(), []),
            ),
        );
    }
}
