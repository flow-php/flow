<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\Calculator\Calculator;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;

final class FlowContextTest extends FlowTestCase
{
    public function test_a_fresh_context_starts_with_an_empty_stream_registry(): void
    {
        static::assertCount(0, flow_context(config())->streams());
    }

    public function test_does_not_share_the_stream_registry_between_contexts_built_from_one_config(): void
    {
        $config = config();

        static::assertNotSame(flow_context($config)->streams(), flow_context($config)->streams());
    }

    public function test_provides_shared_calculator_instance_from_config(): void
    {
        $context = flow_context(config());

        static::assertInstanceOf(Calculator::class, $context->calculator());
        static::assertSame($context->calculator(), $context->calculator());
    }

    public function test_reuses_the_same_stream_registry_within_one_context(): void
    {
        $context = flow_context(config());

        static::assertSame($context->streams(), $context->streams());
    }
}
