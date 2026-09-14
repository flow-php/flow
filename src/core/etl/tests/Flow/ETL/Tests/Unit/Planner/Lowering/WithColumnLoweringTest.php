<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\WithColumn;
use Flow\ETL\Planner\Lowering\WithColumnLowering;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\ScalarFunctionTransformer;

use function array_map;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;

final class WithColumnLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [ScalarFunctionTransformer::class],
            array_map(
                static fn($step) => $step::class,
                (new WithColumnLowering())->steps(
                    new WithColumn(NodeMother::read(), 'doubled', ref('id')->multiply(lit(2))),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }
}
