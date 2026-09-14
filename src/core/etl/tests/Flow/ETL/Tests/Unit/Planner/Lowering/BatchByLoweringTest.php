<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\BatchBy;
use Flow\ETL\Planner\Lowering\BatchByLowering;
use Flow\ETL\Processor\BatchingByProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function array_map;
use function Flow\ETL\DSL\ref;

final class BatchByLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [BatchingByProcessor::class],
            array_map(
                static fn($step) => $step::class,
                (new BatchByLowering())->steps(
                    new BatchBy(NodeMother::read(), ref('id'), 5),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }
}
