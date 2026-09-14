<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Rule;

use Flow\ETL\Extractor\Scannable;
use Flow\ETL\FlowContext;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\ReplaceLeaf;
use Flow\ETL\Planner\LimitWalk;
use Flow\ETL\Planner\Rule;

use function max;

final readonly class PushLimitIntoSource implements Rule
{
    /**
     * One walk per root, pushed only when EVERY walk reaches the source: a limit collected from one consumer
     * must not narrow what another consumer of the same source reads, so the widest one is pushed.
     */
    public function apply(LogicalPlan $plan, FlowContext $context): LogicalPlan
    {
        $leaf = $plan->source();
        $walk = new LimitWalk();
        $limits = [];

        foreach ($plan->consumers() as $node) {
            $limit = $walk->of($node, $leaf);

            if ($limit === null) {
                return $plan;
            }

            $limits[] = $limit;
        }

        if (!$leaf->extractor() instanceof Scannable) {
            return $plan;
        }

        return $plan->transformUp(new ReplaceLeaf($leaf, $leaf->withLimit(max($limits))));
    }
}
