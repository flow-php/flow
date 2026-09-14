<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

use Flow\ETL\FlowContext;

final readonly class Snapshot
{
    public function __construct(
        public LogicalPlan $plan,
        public FlowContext $context,
    ) {}

    public function withRoot(Node $root): self
    {
        return $root === $this->plan->root ? $this : new self($this->plan->withRoot($root), $this->context);
    }
}
