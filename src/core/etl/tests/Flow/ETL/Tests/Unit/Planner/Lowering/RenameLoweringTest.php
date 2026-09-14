<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\Rename;
use Flow\ETL\Planner\Lowering\RenameLowering;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\RenameEntryTransformer;

use function array_map;

final class RenameLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [RenameEntryTransformer::class],
            array_map(
                static fn($step) => $step::class,
                (new RenameLowering())->steps(
                    new Rename(NodeMother::read(), 'id', 'user_id'),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }
}
