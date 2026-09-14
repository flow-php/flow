<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\Until;
use Flow\ETL\Planner\Lowering\UntilLowering;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\UntilTransformer;

use function array_map;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;

final class UntilLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [UntilTransformer::class],
            array_map(
                static fn($step) => $step::class,
                (new UntilLowering())->steps(
                    new Until(NodeMother::read(), ref('id')->equals(lit(1))),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }
}
