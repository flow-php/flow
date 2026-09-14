<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\Batch;
use Flow\ETL\Planner\Lowering\BatchLowering;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function array_map;

final class BatchLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [BatchingProcessor::class],
            array_map(
                static fn($step) => $step::class,
                (new BatchLowering())->steps(new Batch(NodeMother::read(), 10), NodeMother::context(), []),
            ),
        );
    }
}
