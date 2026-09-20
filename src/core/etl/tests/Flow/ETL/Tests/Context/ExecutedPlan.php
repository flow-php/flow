<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\FlowContext;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Rows;
use Generator;

final class ExecutedPlan
{
    /**
     * Plans $plan with the planner of the context's config and executes it with that config's executor.
     *
     * @return Generator<int, Rows>
     */
    public static function of(LogicalPlan $plan, FlowContext $context): Generator
    {
        return $context->config->executor()->execute($context->config->planner()->plan($plan, $context));
    }
}
