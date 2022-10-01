<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline\Execution\Processor;

use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\Execution\LogicalPlan;

interface Processor
{
    public function process(LogicalPlan $plan, FlowContext $context) : LogicalPlan;
}
