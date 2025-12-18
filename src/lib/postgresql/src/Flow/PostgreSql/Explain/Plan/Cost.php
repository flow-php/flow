<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Explain\Plan;

final readonly class Cost
{
    public function __construct(
        private float $startupCost,
        private float $totalCost,
    ) {
    }

    public function incrementalCost() : float
    {
        return $this->totalCost - $this->startupCost;
    }

    public function startupCost() : float
    {
        return $this->startupCost;
    }

    public function totalCost() : float
    {
        return $this->totalCost;
    }
}
