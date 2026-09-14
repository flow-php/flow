<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\Select;
use Flow\ETL\Planner\Lowering\SelectLowering;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\SelectEntriesTransformer;

use function array_map;

final class SelectLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [SelectEntriesTransformer::class],
            array_map(
                static fn($step) => $step::class,
                (new SelectLowering())->steps(new Select(NodeMother::read(), ['id']), NodeMother::context(), []),
            ),
        );
    }
}
