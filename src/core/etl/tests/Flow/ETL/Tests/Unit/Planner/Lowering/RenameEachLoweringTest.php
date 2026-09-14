<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\RenameEach;
use Flow\ETL\Planner\Lowering\RenameEachLowering;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\Rename\RenameMapEntryStrategy;
use Flow\ETL\Transformer\RenameEachEntryTransformer;

use function array_map;

final class RenameEachLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [RenameEachEntryTransformer::class],
            array_map(
                static fn($step) => $step::class,
                (new RenameEachLowering())->steps(
                    new RenameEach(NodeMother::read(), [new RenameMapEntryStrategy(['id' => 'user_id'])]),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }
}
