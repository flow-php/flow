<?php

declare(strict_types=1);

namespace Flow\ETL\Optimizer\Rule;

use Flow\ETL\FlowContext;
use Flow\ETL\Optimizer\LimitWalk;
use Flow\ETL\Optimizer\Rule;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\ReplaceLeaf;

use function max;

final readonly class PushLimitIntoSource implements Rule
{
    /**
     * One walk per consumer, pushed only when EVERY walk reaches the source: a limit collected from one consumer
     * must not narrow what another consumer of the same source reads, so the widest one is pushed.
     */
    public function apply(LogicalPlan $plan, FlowContext $context): LogicalPlan
    {
        $leaf = $plan->source();
        $walk = new LimitWalk();
        $limits = [];

        foreach ($plan->consumerInputs() as $input) {
            $limit = $walk->of($input, $leaf);

            if ($limit === null) {
                return $plan;
            }

            $limits[] = $limit;
        }

        return $plan->transformUp(new ReplaceLeaf($leaf, $leaf->withLimit(max($limits))));
    }
}
