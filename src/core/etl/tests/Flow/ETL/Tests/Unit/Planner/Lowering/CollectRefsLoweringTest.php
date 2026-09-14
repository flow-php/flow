<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\CollectRefs;
use Flow\ETL\Planner\Lowering\CollectRefsLowering;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\CollectReferencesTransformer;

use function array_map;
use function Flow\ETL\DSL\refs;

final class CollectRefsLoweringTest extends FlowTestCase
{
    public function test_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [CollectReferencesTransformer::class],
            array_map(
                static fn($step) => $step::class,
                (new CollectRefsLowering())->steps(
                    new CollectRefs(NodeMother::read(), refs('id')),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }
}
