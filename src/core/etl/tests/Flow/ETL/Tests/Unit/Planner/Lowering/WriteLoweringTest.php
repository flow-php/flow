<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Planner\Lowering\WriteLowering;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\to_memory;

final class WriteLoweringTest extends FlowTestCase
{
    public function test_the_instance_the_node_holds_is_the_step(): void
    {
        $loader = to_memory(new ArrayMemory());

        static::assertSame(
            [$loader],
            (new WriteLowering())->steps(new Write(NodeMother::read(), $loader), NodeMother::context(), []),
        );
    }
}
