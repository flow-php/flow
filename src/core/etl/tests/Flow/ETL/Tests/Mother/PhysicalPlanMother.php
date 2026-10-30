<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\API\DataFrame;
use Flow\ETL\IO\Extractor;
use Flow\ETL\Planning\Physical\PhysicalPlan;

use function Flow\ETL\DSL\config;

final class PhysicalPlanMother
{
    /**
     * The plan the executor would run for this frame, planned the way a trigger plans it.
     */
    public static function of(DataFrame $frame): PhysicalPlan
    {
        $plan = $frame->explain();

        return $plan->planner->plan($plan->logical);
    }

    public static function reading(Extractor $extractor): PhysicalPlan
    {
        return config()->planner()->plan(NodeMother::plan(NodeMother::read($extractor)));
    }
}
