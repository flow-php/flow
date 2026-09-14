<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\DuplicateRow;
use Flow\ETL\Planner\Lowering\DuplicateRowLowering;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\DuplicateRowTransformer;
use Flow\ETL\WithEntry;

use function array_map;
use function Flow\ETL\DSL\lit;

final class DuplicateRowLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [DuplicateRowTransformer::class],
            array_map(
                static fn($step) => $step::class,
                (new DuplicateRowLowering())->steps(new DuplicateRow(NodeMother::read(), lit(true), [new WithEntry(
                    'copy',
                    lit(1),
                )]), NodeMother::context(), []),
            ),
        );
    }
}
