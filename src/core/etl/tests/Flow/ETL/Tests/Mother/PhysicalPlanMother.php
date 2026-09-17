<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Executor\PhysicalPlan;
use Flow\ETL\Extractor;
use Flow\ETL\Optimizer;
use Flow\ETL\Planner;

final class PhysicalPlanMother
{
    public static function reading(Extractor $extractor): PhysicalPlan
    {
        return (new Planner(Optimizer::default()))->plan(
            NodeMother::plan(NodeMother::read($extractor)),
            NodeMother::context(),
        );
    }
}
