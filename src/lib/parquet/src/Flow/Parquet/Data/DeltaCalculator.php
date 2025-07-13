<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

final readonly class DeltaCalculator
{
    public function calculateDelta(int $previous, int $current) : int
    {
        // Check if simple subtraction would overflow to float
        $result = $current - $previous;

        /**
         * PHP will convert int overflow to float.
         *
         * @phpstan-ignore-next-line function.impossibleType
         */
        if (\is_float($result)) {
            // Use BCMath for precise calculation without overflow
            $deltaString = \bcsub((string) $current, (string) $previous, 0);

            // For 64-bit systems, implement proper 2's complement wrapping
            if (PHP_INT_SIZE === 8) {
                // If delta is out of range, wrap it using 2^64
                while (\bccomp($deltaString, (string) PHP_INT_MAX, 0) > 0) {
                    $deltaString = \bcsub($deltaString, '18446744073709551616', 0); // 2^64
                }

                while (\bccomp($deltaString, (string) PHP_INT_MIN, 0) < 0) {
                    $deltaString = \bcadd($deltaString, '18446744073709551616', 0); // 2^64
                }
            } else {
                // For 32-bit systems
                while (\bccomp($deltaString, (string) PHP_INT_MAX, 0) > 0) {
                    $deltaString = \bcsub($deltaString, '4294967296', 0); // 2^32
                }

                while (\bccomp($deltaString, (string) PHP_INT_MIN, 0) < 0) {
                    $deltaString = \bcadd($deltaString, '4294967296', 0); // 2^32
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
    public function calculateDeltas(array $values) : array
    {
        $deltas = [];
        $valuesCount = count($values);

        for ($i = 1; $i < $valuesCount; $i++) {
            $deltas[] = $this->calculateDelta($values[$i - 1], $values[$i]);
        }

        return $deltas;
    }

    public function calculateRelativeDelta(int $delta, int $minDelta) : int
    {
        // Check if simple subtraction would overflow to float
        $result = $delta - $minDelta;

        /**
         * PHP will convert int overflow to float.
         *
         * @phpstan-ignore-next-line function.impossibleType
         */
        if (\is_float($result)) {
            // Use BCMath for precise calculation without overflow
            $deltaString = \bcsub((string) $delta, (string) $minDelta, 0);

            // For 64-bit systems, implement proper 2's complement wrapping
            if (PHP_INT_SIZE === 8) {
                // If delta is out of range, wrap it using 2^64
                while (\bccomp($deltaString, (string) PHP_INT_MAX, 0) > 0) {
                    $deltaString = \bcsub($deltaString, '18446744073709551616', 0); // 2^64
                }

                while (\bccomp($deltaString, (string) PHP_INT_MIN, 0) < 0) {
                    $deltaString = \bcadd($deltaString, '18446744073709551616', 0); // 2^64
                }
            } else {
                // For 32-bit systems
                while (\bccomp($deltaString, (string) PHP_INT_MAX, 0) > 0) {
                    $deltaString = \bcsub($deltaString, '4294967296', 0); // 2^32
                }

                while (\bccomp($deltaString, (string) PHP_INT_MIN, 0) < 0) {
                    $deltaString = \bcadd($deltaString, '4294967296', 0); // 2^32
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
    public function reconstructValues(int $firstValue, array $deltas) : array
    {
        $values = [$firstValue];
        $currentValue = $firstValue;

        foreach ($deltas as $delta) {
            // Check if simple addition would overflow to float
            $result = $currentValue + $delta;

            if (\is_float($result)) {
                // Use BCMath for precise calculation without overflow
                $nextValueString = \bcadd((string) $currentValue, (string) $delta, 0);

                // For 64-bit systems, implement proper 2's complement wrapping
                if (PHP_INT_SIZE === 8) {
                    // If result is out of range, wrap it using 2^64
                    while (\bccomp($nextValueString, (string) PHP_INT_MAX, 0) > 0) {
                        $nextValueString = \bcsub($nextValueString, '18446744073709551616', 0); // 2^64
                    }

                    while (\bccomp($nextValueString, (string) PHP_INT_MIN, 0) < 0) {
                        $nextValueString = \bcadd($nextValueString, '18446744073709551616', 0); // 2^64
                    }
                } else {
                    // For 32-bit systems
                    while (\bccomp($nextValueString, (string) PHP_INT_MAX, 0) > 0) {
                        $nextValueString = \bcsub($nextValueString, '4294967296', 0); // 2^32
                    }

                    while (\bccomp($nextValueString, (string) PHP_INT_MIN, 0) < 0) {
                        $nextValueString = \bcadd($nextValueString, '4294967296', 0); // 2^32
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
}
