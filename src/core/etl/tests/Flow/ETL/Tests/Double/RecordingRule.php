<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use ArrayObject;
use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Optimizer\Rule;
use Flow\ETL\Values\Column\Backend;

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

    public function apply(LogicalPlan $plan, Backend $backend): LogicalPlan
    {
        $this->log[] = $this->name;

        return $plan;
    }
}
