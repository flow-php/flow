<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\Repartition;
use Flow\ETL\Planner\Lowering\RepartitionLowering;
use Flow\ETL\Processor\BucketingProcessor;
use Flow\ETL\Processor\RepartitionProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function array_map;
use function Flow\ETL\DSL\refs;

final class RepartitionLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [BucketingProcessor::class, RepartitionProcessor::class],
            array_map(
                static fn($step) => $step::class,
                (new RepartitionLowering())->steps(
                    new Repartition(NodeMother::read(), refs('id')),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }
}
