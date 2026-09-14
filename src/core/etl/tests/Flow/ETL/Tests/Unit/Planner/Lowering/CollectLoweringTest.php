<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\Collect;
use Flow\ETL\Planner\Lowering\CollectLowering;
use Flow\ETL\Processor\CollectingProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function array_map;

final class CollectLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [CollectingProcessor::class],
            array_map(
                static fn($step) => $step::class,
                (new CollectLowering())->steps(new Collect(NodeMother::read()), NodeMother::context(), []),
            ),
        );
    }
}
