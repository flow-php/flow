<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\WindowColumn;
use Flow\ETL\Planner\Lowering\WindowColumnLowering;
use Flow\ETL\Processor\BucketingProcessor;
use Flow\ETL\Processor\CollectingProcessor;
use Flow\ETL\Processor\RepartitionProcessor;
use Flow\ETL\Processor\WindowProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function array_map;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\window;

final class WindowColumnLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [CollectingProcessor::class, WindowProcessor::class],
            array_map(
                static fn($step) => $step::class,
                (new WindowColumnLowering())->steps(
                    new WindowColumn(NodeMother::read(), 'rank', rank()->over(window()->orderBy(ref('id')))),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }

    public function test_a_partitioned_window_lowers_to_repartition_steps_then_the_window_processor(): void
    {
        static::assertSame(
            [BucketingProcessor::class, RepartitionProcessor::class, WindowProcessor::class],
            array_map(
                static fn($step) => $step::class,
                (new WindowColumnLowering())->steps(
                    new WindowColumn(
                        NodeMother::read(),
                        'rank',
                        rank()->over(window()->partitionBy(ref('group'))->orderBy(ref('id'))),
                    ),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }
}
