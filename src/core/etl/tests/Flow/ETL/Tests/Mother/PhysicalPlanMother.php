<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\DataFrame;
use Flow\ETL\Executor\PhysicalPlan;
use Flow\ETL\Extractor;
use Flow\ETL\Optimizer;
use Flow\ETL\Planner;

final class PhysicalPlanMother
{
    /**
     * The plan the executor would run for this frame, planned the way a trigger plans it.
     */
    public static function of(DataFrame $frame): PhysicalPlan
    {
        $plan = $frame->explain();

        return $plan->context->config->planner()->plan($plan->logical, $plan->context);
    }

    public static function reading(Extractor $extractor): PhysicalPlan
    {
        return (new Planner(Optimizer::default()))->plan(
            NodeMother::plan(NodeMother::read($extractor)),
            NodeMother::context(),
        );
    }
}
