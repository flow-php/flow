<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Explain\Plan;

final readonly class Cost
{
    public function __construct(
        private float $startupCost,
        private float $totalCost,
    ) {}

    /**
     * @param array{startup_cost: float, total_cost: float} $data
     */
    public static function fromArray(array $data): self
    {
        return new self(startupCost: $data['startup_cost'], totalCost: $data['total_cost']);
    }

    public function incrementalCost(): float
    {
        return $this->totalCost - $this->startupCost;
    }

    /**
     * @return array{startup_cost: float, total_cost: float}
     */
    public function normalize(): array
    {
        return [
            'startup_cost' => $this->startupCost,
            'total_cost' => $this->totalCost,
        ];
    }

    public function startupCost(): float
    {
        return $this->startupCost;
    }

    public function totalCost(): float
    {
        return $this->totalCost;
    }
}
