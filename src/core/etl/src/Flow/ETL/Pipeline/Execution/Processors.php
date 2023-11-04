<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline\Execution;

use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\Execution\Processor\Processor;

final class Processors
{
    /**
     * @var array<Processor>
     */
    private readonly array $processors;

    public function __construct(Processor ...$processors)
    {
        $this->processors = $processors;
    }

    public function process(ExecutionPlan $plan, FlowContext $context) : ExecutionPlan
    {
        $processedPlan = $plan;

        foreach ($this->processors as $processor) {
            $processedPlan = $processor->process($processedPlan, $context);
        }

        return $processedPlan;
    }
}
