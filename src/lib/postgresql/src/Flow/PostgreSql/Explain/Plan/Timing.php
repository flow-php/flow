<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Explain\Plan;

final readonly class Timing
{
    public function __construct(
        private float $startupTime,
        private float $totalTime,
        private int $loops,
    ) {
    }

    public function averageTime() : float
    {
        return $this->loops > 0 ? $this->totalTime / $this->loops : 0.0;
    }

    public function loops() : int
    {
        return $this->loops;
    }

    public function startupTime() : float
    {
        return $this->startupTime;
    }

    public function totalActualTime() : float
    {
        return $this->totalTime * $this->loops;
    }

    public function totalTime() : float
    {
        return $this->totalTime;
    }
}
