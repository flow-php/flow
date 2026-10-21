<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

use function abs;
use function bcadd;
use function bccomp;
use function bcsub;
use function is_float;
use function max;
use function min;

final readonly class DeltaCalculator
{
    /**
     * Values within ±2^61 add and subtract without integer overflow, so they skip the BCMath fallback.
     */
    private const int OVERFLOW_FREE_MAGNITUDE = 1 << 61;

    public function calculateDelta(int $previous, int $current): int
    {
        $result = $current - $previous;

        // @mago-ignore analysis:impossible-condition
        if (is_float($result)) {
            // Use BCMath for precise calculation without overflow
            $deltaString = bcsub((string) $current, (string) $previous, 0);

            // For 64-bit systems, implement proper 2's complement wrapping
            // @mago-ignore analysis:redundant-condition
            // @mago-ignore analysis:redundant-comparison
            if (PHP_INT_SIZE === 8) {
                // If delta is out of range, wrap it using 2^64
                while (bccomp($deltaString, (string) PHP_INT_MAX, 0) > 0) {
                    $deltaString = bcsub($deltaString, '18446744073709551616', 0); // 2^64
                }

                while (bccomp($deltaString, (string) PHP_INT_MIN, 0) < 0) {
                    $deltaString = bcadd($deltaString, '18446744073709551616', 0); // 2^64
                }
            } else {
                // For 32-bit systems
                while (bccomp($deltaString, (string) PHP_INT_MAX, 0) > 0) {
                    $deltaString = bcsub($deltaString, '4294967296', 0); // 2^32
                }

                while (bccomp($deltaString, (string) PHP_INT_MIN, 0) < 0) {
                    $deltaString = bcadd($deltaString, '4294967296', 0); // 2^32
                }
            }

            return (int) $deltaString;
        }

        return (int) $result;
    }

    /**
     * @param array<int> $values
     *
     * @return array<int>
     */
    public function calculateDeltas(array $values): array
    {
        $deltas = [];
        $valuesCount = count($values);

        if ($valuesCount < 2) {
            return $deltas;
        }

        if (min($values) >= -self::OVERFLOW_FREE_MAGNITUDE && max($values) <= self::OVERFLOW_FREE_MAGNITUDE) {
            for ($i = 1; $i < $valuesCount; $i++) {
                $deltas[] = $values[$i] - $values[$i - 1];
            }

            return $deltas;
        }

        for ($i = 1; $i < $valuesCount; $i++) {
            $deltas[] = $this->calculateDelta($values[$i - 1], $values[$i]);
        }

        return $deltas;
    }

    /**
     * @param array<int> $relativeDeltas
     *
     * @return array<int>
     */
    public function calculateAbsoluteDeltas(array $relativeDeltas, int $minDelta): array
    {
        $deltas = [];

        if (
            $relativeDeltas === []
            || abs($minDelta) <= self::OVERFLOW_FREE_MAGNITUDE
            && max($relativeDeltas) <= self::OVERFLOW_FREE_MAGNITUDE
            && min($relativeDeltas) >= -self::OVERFLOW_FREE_MAGNITUDE
        ) {
            foreach ($relativeDeltas as $relativeDelta) {
                $deltas[] = $relativeDelta + $minDelta;
            }

            return $deltas;
        }

        foreach ($relativeDeltas as $relativeDelta) {
            // PHP converts int overflow to float at runtime, so $result may be float even though both operands are int.
            $result = $relativeDelta + $minDelta;

            // @mago-ignore analysis:impossible-condition
            if (is_float($result)) {
                $preciseResult = bcadd((string) $relativeDelta, (string) $minDelta, 0);

                if (bccomp($preciseResult, (string) PHP_INT_MAX, 0) > 0) {
                    $preciseResult = bcsub($preciseResult, '18446744073709551616', 0);
                } elseif (bccomp($preciseResult, (string) PHP_INT_MIN, 0) < 0) {
                    $preciseResult = bcadd($preciseResult, '18446744073709551616', 0);
                }

                $result = (int) $preciseResult;
            }

            $deltas[] = $result;
        }

        return $deltas;
    }

    public function calculateRelativeDelta(int $delta, int $minDelta): int
    {
        $result = $delta - $minDelta;

        // @mago-ignore analysis:impossible-condition
        if (is_float($result)) {
            // Use BCMath for precise calculation without overflow
            $deltaString = bcsub((string) $delta, (string) $minDelta, 0);

            // For 64-bit systems, implement proper 2's complement wrapping
            // @mago-ignore analysis:redundant-condition
            // @mago-ignore analysis:redundant-comparison
            if (PHP_INT_SIZE === 8) {
                // If delta is out of range, wrap it using 2^64
                while (bccomp($deltaString, (string) PHP_INT_MAX, 0) > 0) {
                    $deltaString = bcsub($deltaString, '18446744073709551616', 0); // 2^64
                }

                while (bccomp($deltaString, (string) PHP_INT_MIN, 0) < 0) {
                    $deltaString = bcadd($deltaString, '18446744073709551616', 0); // 2^64
                }
            } else {
                // For 32-bit systems
                while (bccomp($deltaString, (string) PHP_INT_MAX, 0) > 0) {
                    $deltaString = bcsub($deltaString, '4294967296', 0); // 2^32
                }

                while (bccomp($deltaString, (string) PHP_INT_MIN, 0) < 0) {
                    $deltaString = bcadd($deltaString, '4294967296', 0); // 2^32
                }
            }

            return (int) $deltaString;
        }

        return (int) $result;
    }

    /**
     * @param array<int> $deltas
     *
     * @return array<int>
     */
    public function calculateRelativeDeltas(array $deltas, int $minDelta): array
    {
        $relativeDeltas = [];

        if (
            $deltas === []
            || $minDelta >= -self::OVERFLOW_FREE_MAGNITUDE && max($deltas) <= self::OVERFLOW_FREE_MAGNITUDE
        ) {
            foreach ($deltas as $delta) {
                $relativeDeltas[] = $delta - $minDelta;
            }

            return $relativeDeltas;
        }

        foreach ($deltas as $delta) {
            $relativeDeltas[] = $this->calculateRelativeDelta($delta, $minDelta);
        }

        return $relativeDeltas;
    }

    /**
     * @param array<int> $deltas
     *
     * @return array<int>
     */
    public function reconstructValues(int $firstValue, array $deltas): array
    {
        $values = [$firstValue];
        $currentValue = $firstValue;

        foreach ($deltas as $delta) {
            // PHP converts int overflow to float at runtime, so $result may be float even though both operands are int.
            $result = $currentValue + $delta;

            // @mago-ignore analysis:impossible-condition
            if (is_float($result)) {
                // Use BCMath for precise calculation without overflow
                $nextValueString = bcadd((string) $currentValue, (string) $delta, 0);

                // For 64-bit systems, implement proper 2's complement wrapping
                // @mago-ignore analysis:redundant-condition
                // @mago-ignore analysis:redundant-comparison
                if (PHP_INT_SIZE === 8) {
                    // If result is out of range, wrap it using 2^64
                    while (bccomp($nextValueString, (string) PHP_INT_MAX, 0) > 0) {
                        $nextValueString = bcsub($nextValueString, '18446744073709551616', 0); // 2^64
                    }

                    while (bccomp($nextValueString, (string) PHP_INT_MIN, 0) < 0) {
                        $nextValueString = bcadd($nextValueString, '18446744073709551616', 0); // 2^64
                    }
                } else {
                    // For 32-bit systems
                    while (bccomp($nextValueString, (string) PHP_INT_MAX, 0) > 0) {
                        $nextValueString = bcsub($nextValueString, '4294967296', 0); // 2^32
                    }

                    while (bccomp($nextValueString, (string) PHP_INT_MIN, 0) < 0) {
                        $nextValueString = bcadd($nextValueString, '4294967296', 0); // 2^32
                    }
                }

                $currentValue = (int) $nextValueString;
            } else {
                $currentValue = (int) $result;
            }

            $values[] = $currentValue;
        }

        return $values;
    }

    public function toInt32(int $value): int
    {
        $unsigned = $value & 0xFFFFFFFF;

        return $unsigned >= 0x80000000 ? $unsigned - 0x100000000 : $unsigned;
    }
}
