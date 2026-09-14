<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\Discard;
use Flow\ETL\Planner\Lowering\DiscardLowering;
use Flow\ETL\Processor\VoidProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function array_map;

final class DiscardLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [VoidProcessor::class],
            array_map(
                static fn($step) => $step::class,
                (new DiscardLowering())->steps(new Discard(NodeMother::read()), NodeMother::context(), []),
            ),
        );
    }
}
