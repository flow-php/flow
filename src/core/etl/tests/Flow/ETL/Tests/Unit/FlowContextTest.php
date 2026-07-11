<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\Calculator\Calculator;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;

final class FlowContextTest extends FlowTestCase
{
    public function test_provides_shared_calculator_instance_from_config(): void
    {
        $context = flow_context(config());

        static::assertInstanceOf(Calculator::class, $context->calculator());
        static::assertSame($context->calculator(), $context->calculator());
    }
}
