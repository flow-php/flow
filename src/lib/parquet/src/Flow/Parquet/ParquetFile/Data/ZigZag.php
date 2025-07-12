<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

final readonly class ZigZag
{
    /**
     * Decode a ZigZag encoded value back to signed integer.
     *
     * Implementation follows Protocol Buffers specification using logical right shift
     */
    public function decode(int $value) : int
    {
        // Use logical right shift and handle the sign bit correctly
        return $this->logicalRightShift($value, 1) ^ (-($value & 1));
    }

    /**
     * Encode a signed integer using ZigZag encoding.
     *
     * ZigZag encoding maps signed integers to unsigned integers so that
     * numbers with a small absolute value have a small encoded value.
     *
     * Maps: 0->0, -1->1, 1->2, -2->3, 2->4, -3->5, ...
     *
     * @see https://protobuf.dev/programming-guides/encoding/
     */
    public function encode(int $value) : int
    {
        // Use the mathematical approach that works correctly for all values:
        // For positive values: 2 * value
        // For negative values: 2 * abs(value) - 1
        if ($value >= 0) {
            // For non-negative values, just shift left by 1
            return $this->safeLeftShift($value, 1);
        }
        // For negative values: encode as 2 * |value| - 1
        // This maps -1->1, -2->3, -3->5, etc.

        // Handle PHP_INT_MIN special case where -$value would overflow to float
        if ($value === PHP_INT_MIN) {
            // For PHP_INT_MIN, use BCMath to handle the absolute value calculation
            $absValue = \bcsub('0', (string) $value, 0); // |PHP_INT_MIN|
            $doubled = \bcmul($absValue, '2', 0);
            $result = \bcsub($doubled, '1', 0);

            return $this->wrapTo64BitSigned($result);
        }

        return $this->safeLeftShift(-$value, 1) - 1;

    }

    /**
     * Perform logical (unsigned) right shift.
     */
    private function logicalRightShift(int $value, int $bits) : int
    {
        if (PHP_INT_SIZE === 8 && $bits === 1) {
            // For 64-bit systems, handle logical right shift
            if ($value < 0) {
                // Convert to unsigned representation, shift, then back to signed
                // This effectively does an unsigned right shift
                return (($value & 0x7FFFFFFFFFFFFFFF) >> 1) | (0x4000000000000000);
            }
        }

        return $value >> $bits;
    }

    /**
     * Safely perform left shift that handles overflow for large values.
     */
    private function safeLeftShift(int $value, int $bits) : int
    {
        if (PHP_INT_SIZE === 8 && $bits === 1) {
            // Check if shifting left by 1 would cause overflow
            if ($value > (PHP_INT_MAX >> 1)) {
                // Use BCMath to handle the overflow case
                $result = \bcmul((string) $value, '2', 0);

                return $this->wrapTo64BitSigned($result);
            }
        }

        return $value << $bits;
    }

    /**
     * Wrap a BCMath string result to 64-bit signed integer range.
     */
    private function wrapTo64BitSigned(string $value) : int
    {
        // For 64-bit systems, wrap the value to fit in signed 64-bit range
        while (\bccomp($value, '9223372036854775807', 0) > 0) {
            $value = \bcsub($value, '18446744073709551616', 0); // 2^64
        }

        while (\bccomp($value, '-9223372036854775808', 0) < 0) {
            $value = \bcadd($value, '18446744073709551616', 0); // 2^64
        }

        return (int) $value;
    }
}
