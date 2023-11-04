<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline\Execution\Processor;

use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\Execution\ExecutionPlan;

interface Processor
{
    public function process(ExecutionPlan $plan, FlowContext $context) : ExecutionPlan;
}
