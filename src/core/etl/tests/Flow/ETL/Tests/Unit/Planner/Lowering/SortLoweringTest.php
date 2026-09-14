<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\Sort;
use Flow\ETL\Planner\Lowering\SortLowering;
use Flow\ETL\Processor\MemorySortProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function array_map;
use function Flow\ETL\DSL\memory_sort;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;

final class SortLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [MemorySortProcessor::class],
            array_map(
                static fn($step) => $step::class,
                (new SortLowering())->steps(
                    new Sort(NodeMother::read(), refs(ref('id')), memory_sort()),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }
}
