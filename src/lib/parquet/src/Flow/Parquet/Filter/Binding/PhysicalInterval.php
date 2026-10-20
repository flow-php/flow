<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding;

/**
 * A null bound is unbounded.
 */
final readonly class PhysicalInterval
{
    public function __construct(
        public bool|float|int|string|null $lower,
        public bool $lowerInclusive,
        public bool|float|int|string|null $upper,
        public bool $upperInclusive,
    ) {}

    public static function unbounded(): self
    {
        return new self(null, false, null, false);
    }
}
