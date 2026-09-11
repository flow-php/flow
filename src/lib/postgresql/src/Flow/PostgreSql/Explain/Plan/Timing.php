<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Explain\Plan;

/**
 * @type TimingShape = array{startup_time: float, total_time: float, loops: int}
 */
final readonly class Timing
{
    public function __construct(
        private float $startupTime,
        private float $totalTime,
        private int $loops,
    ) {}

    /**
     * @param TimingShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(startupTime: $data['startup_time'], totalTime: $data['total_time'], loops: $data['loops']);
    }

    public function averageTime(): float
    {
        return $this->loops > 0 ? $this->totalTime / $this->loops : 0.0;
    }

    public function loops(): int
    {
        return $this->loops;
    }

    /**
     * @return TimingShape
     */
    public function normalize(): array
    {
        return [
            'startup_time' => $this->startupTime,
            'total_time' => $this->totalTime,
            'loops' => $this->loops,
        ];
    }

    public function startupTime(): float
    {
        return $this->startupTime;
    }

    public function totalActualTime(): float
    {
        return $this->totalTime * $this->loops;
    }

    public function totalTime(): float
    {
        return $this->totalTime;
    }
}
