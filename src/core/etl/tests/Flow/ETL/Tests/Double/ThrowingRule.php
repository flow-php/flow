<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Optimizer\Rule;
use Flow\ETL\Values\Column\Backend;
use Throwable;

final readonly class ThrowingRule implements Rule
{
    public function __construct(
        private Throwable $throws,
    ) {}

    public function apply(LogicalPlan $plan, Backend $backend): LogicalPlan
    {
        throw $this->throws;
    }
}
