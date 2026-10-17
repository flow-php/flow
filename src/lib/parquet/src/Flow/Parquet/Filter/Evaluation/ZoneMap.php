<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Evaluation;

/**
 * $min and $max are decoded physical values the column order trusts, null when there is none. $allNaN: every value
 * that is not null is NaN.
 */
final readonly class ZoneMap
{
    public function __construct(
        public bool|float|int|string|null $min,
        public bool|float|int|string|null $max,
        public bool $minExact,
        public bool $maxExact,
        public ?int $nullCount,
        public ?int $nanCount,
        public int $rowCount,
        public bool $allNaN,
    ) {}
}
