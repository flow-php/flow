<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\Drop;
use Flow\ETL\Planner\Lowering\DropLowering;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\DropEntriesTransformer;

use function array_map;

final class DropLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [DropEntriesTransformer::class],
            array_map(
                static fn($step) => $step::class,
                (new DropLowering())->steps(new Drop(NodeMother::read(), ['id']), NodeMother::context(), []),
            ),
        );
    }
}
