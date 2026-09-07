<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Exception;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Tests\FlowTestCase;

final class InvalidLogicExceptionTest extends FlowTestCase
{
    public function test_cyclic_plan_names_the_operation_and_the_way_out(): void
    {
        static::assertSame(
            'Cannot describe this plan: it reads from a DataFrame that reads back from it. A DataFrame is '
            . 'mutable, so join(), select() and withEntry() can add that edge after both frames exist. Break '
            . 'the cycle by reading the nested frame from its own source.',
            InvalidLogicException::cyclicPlanOnDescribe()->getMessage(),
        );
        static::assertSame(
            'Cannot run this plan: it reads from a DataFrame that reads back from it. A DataFrame is '
            . 'mutable, so join(), select() and withEntry() can add that edge after both frames exist. Break '
            . 'the cycle by reading the nested frame from its own source.',
            InvalidLogicException::cyclicPlanOnRun()->getMessage(),
        );
    }
}
