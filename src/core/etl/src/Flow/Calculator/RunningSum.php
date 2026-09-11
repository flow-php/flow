<?php

declare(strict_types=1);

namespace Flow\Calculator;

use function is_string;

final readonly class RunningSum
{
    public function __construct(
        private Calculator $calculator = new Calculator(),
    ) {}

    /**
     * @param float|int|numeric-string $value
     */
    public function add(float|int $sum, float|int|string $value, bool $exact): float|int
    {
        if ($exact) {
            return $this->calculator->add($sum, $value);
        }

        return $sum + (is_string($value) ? (float) $value : $value);
    }
}
