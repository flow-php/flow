<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use ArrayObject;
use Flow\ETL\FlowContext;
use Flow\ETL\Optimizer\Rule;
use Flow\ETL\Plan\LogicalPlan;

/**
 * Records into a log shared between rules, so a test can read the order the planner applied them in.
 */
final readonly class RecordingRule implements Rule
{
    /**
     * @param ArrayObject<int, string> $log
     */
    public function __construct(
        private string $name,
        private ArrayObject $log,
    ) {}

    public function apply(LogicalPlan $plan, FlowContext $context): LogicalPlan
    {
        $this->log[] = $this->name;

        return $plan;
    }
}
