<?php

declare(strict_types=1);

namespace Flow\Calculator;

use function abs;
use function is_finite;
use function is_float;
use function is_int;
use function is_string;

/**
 * IEEE addition with Neumaier compensation: the rounding error of every float addition is carried and added back once,
 * so a long run of floats does not drift. An exact addition is decimal (Calculator) over the compensated total.
 */
final class RunningSum
{
    private float $compensation = 0.0;

    private float|int $sum = 0;

    public function __construct(
        private readonly Calculator $calculator = new Calculator(),
    ) {}

    /**
     * @param float|int|numeric-string $value
     */
    public function add(float|int|string $value, bool $exact): void
    {
        if ($exact) {
            $this->sum = $this->calculator->add($this->value(), $value);
            $this->compensation = 0.0;

            return;
        }

        $value = is_string($value) ? (float) $value : $value;

        if (is_int($this->sum) && is_int($value)) {
            // an int overflow promotes to float, as PHP's + does
            $this->sum += $value;

            return;
        }

        $sum = (float) $this->sum;
        $total = $sum + $value;

        if (is_finite($total)) {
            $this->compensation += abs($sum) >= abs($value) ? $sum - $total + $value : $value - $total + $sum;
        }

        $this->sum = $total;
    }

    public function merge(self $other, bool $exact): void
    {
        if ($exact) {
            $this->add($other->value(), true);

            return;
        }

        $this->add($other->sum, false);
        $this->compensation += $other->compensation;
    }

    public function value(): float|int
    {
        return is_float($this->sum) && is_finite($this->sum) ? $this->sum + $this->compensation : $this->sum;
    }
}
