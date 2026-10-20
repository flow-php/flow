<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding;

use function bcadd;
use function bccomp;
use function bcdiv;
use function bcmod;
use function bcmul;
use function bcsub;

/**
 * The lowest and highest physical value whose decoded value equals a literal, low above high when none does.
 */
final readonly class EqualRange
{
    public function __construct(
        public bool|float|int|string|OutOfDomain $low,
        public bool|float|int|string|OutOfDomain $high,
    ) {}

    /**
     * Physical value p decodes to p * $step: the value is hit only on the grid.
     *
     * @param numeric-string $value
     * @param numeric-string $step
     */
    public static function grid(string $value, string $step, int $min, int $max): self
    {
        $floor = bcdiv($value, $step, 0);

        if (bccomp(bcmod($value, $step), '0') < 0) {
            $floor = bcsub($floor, '1');
        }

        return self::integers(
            bccomp(bcmul($floor, $step), $value) === 0 ? $floor : bcadd($floor, '1'),
            $floor,
            $min,
            $max,
        );
    }

    /**
     * @param numeric-string $low
     * @param numeric-string $high
     */
    public static function integers(string $low, string $high, int $min, int $max): self
    {
        return new self(self::edge($low, $min, $max), self::edge($high, $min, $max));
    }

    public static function point(bool|float|int|string|OutOfDomain $value): self
    {
        return new self($value, $value);
    }

    /**
     * @param numeric-string $value
     */
    private static function edge(string $value, int $min, int $max): int|OutOfDomain
    {
        return match (true) {
            bccomp($value, (string) $min) < 0 => OutOfDomain::Below,
            bccomp($value, (string) $max) > 0 => OutOfDomain::Above,
            default => (int) $value,
        };
    }
}
