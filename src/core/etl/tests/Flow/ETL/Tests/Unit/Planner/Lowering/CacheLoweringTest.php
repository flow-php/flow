<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\Cache;
use Flow\ETL\Planner\Lowering\CacheLowering;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Processor\CachingProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function array_map;

final class CacheLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [CachingProcessor::class],
            array_map(
                static fn($step) => $step::class,
                (new CacheLowering())->steps(
                    new Cache(NodeMother::read(), 'id', null, null),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }

    public function test_a_batch_size_prepends_a_batching_processor(): void
    {
        static::assertSame(
            [BatchingProcessor::class, CachingProcessor::class],
            array_map(
                static fn($step) => $step::class,
                (new CacheLowering())->steps(new Cache(NodeMother::read(), 'id', 100, null), NodeMother::context(), []),
            ),
        );
    }
}
