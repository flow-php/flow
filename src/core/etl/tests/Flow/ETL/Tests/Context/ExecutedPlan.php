<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Values\Rows;
use Generator;

final class ExecutedPlan
{
    /**
     * Executes $plan with the executor of the context's config, which plans it with that config's planner.
     *
     * @return Generator<int, Rows>
     */
    public static function of(LogicalPlan $plan, FlowContext $context): Generator
    {
        return $context->config->executor()->execute($plan, $context);
    }
}
