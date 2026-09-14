<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\Filter;
use Flow\ETL\Planner\Lowering\FilterLowering;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\ScalarFunctionFilterTransformer;

use function array_map;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;

final class FilterLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [ScalarFunctionFilterTransformer::class],
            array_map(
                static fn($step) => $step::class,
                (new FilterLowering())->steps(
                    new Filter(NodeMother::read(), ref('id')->equals(lit(1))),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }
}
