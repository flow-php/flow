<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\ErrorHandler\IgnoreError;
use Flow\ETL\ErrorHandler\ThrowError;
use Flow\ETL\FlowContext;
use Flow\ETL\Optimizer;
use Flow\ETL\Optimizer\Rule\CombineLimits;
use Flow\ETL\Plan;
use Flow\ETL\Plan\Explain;
use Flow\ETL\Plan\Format;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Stage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\config_builder;

final class PlanTest extends FlowTestCase
{
    public function test_of_keeps_the_logical_plan_and_the_config(): void
    {
        $logical = NodeMother::plan(NodeMother::read());
        $context = NodeMother::context();

        $plan = Plan::of($logical, $context);

        static::assertSame($logical, $plan->logical);
        static::assertSame($context->config, $plan->context->config);
    }

    public function test_of_carries_the_current_error_handler_in_a_new_context(): void
    {
        $context = NodeMother::context();
        $context->setErrorHandler($handler = new IgnoreError());

        $plan = Plan::of(NodeMother::plan(NodeMother::read()), $context);

        static::assertNotSame($context, $plan->context);
        static::assertSame($handler, $plan->context->errorHandler());
    }

    public function test_a_handler_set_afterwards_does_not_reach_the_plans_context(): void
    {
        $context = NodeMother::context();
        $plan = Plan::of(NodeMother::plan(NodeMother::read()), $context);

        $context->setErrorHandler(new IgnoreError());

        static::assertInstanceOf(ThrowError::class, $plan->context->errorHandler());
    }

    public function test_its_context_has_its_own_telemetry_context(): void
    {
        $context = NodeMother::context();

        static::assertNotSame(
            $context->telemetry(),
            Plan::of(NodeMother::plan(NodeMother::read()), $context)->context->telemetry(),
        );
    }

    public function test_to_string_prints_the_optimized_tree_by_default(): void
    {
        $plan = Plan::of(
            NodeMother::plan(new Limit(new Limit(NodeMother::read(), 5), 3)),
            new FlowContext(config_builder()->optimizer(new Optimizer(new CombineLimits()))->build()),
        );

        static::assertSame((new Explain())->of($plan, Stage::optimized, Format::tree), $plan->toString());
    }

    public function test_to_string_prints_the_requested_stage_and_format(): void
    {
        $plan = Plan::of(
            NodeMother::plan(new Limit(new Limit(NodeMother::read(), 5), 3)),
            new FlowContext(config_builder()->optimizer(new Optimizer(new CombineLimits()))->build()),
        );

        static::assertSame(
            (new Explain())->of($plan, Stage::unoptimized, Format::boxes),
            $plan->toString(Stage::unoptimized, Format::boxes),
        );
        static::assertNotSame($plan->toString(), $plan->toString(Stage::unoptimized));
    }
}
