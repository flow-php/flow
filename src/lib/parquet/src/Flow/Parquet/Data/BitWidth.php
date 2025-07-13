<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

final class BitWidth
{
    public static function calculate(int $value) : int
    {
        return (int) \ceil(\log($value + 1, 2));
    }

    /**
     * @param array<int> $ints
     */
    public static function fromArray(array $ints) : int
    {
        if (!\count($ints)) {
            return 0;
        }

        $maxInt = \max($ints);
        $minInt = \min($ints);

        // If all values are 0, no bits needed
        if ($maxInt === 0 && $minInt === 0) {
            return 0;
        }

        // If we have negative values, we need to treat them as unsigned
        // or use the full bit width. For delta encoding, relative deltas
        // should always be non-negative, but due to overflow we might get
        // negative wrapped values that need to be treated as large unsigned values.
        if ($minInt < 0) {
            // For negative values in the range, we need full 64-bit width
            // because they represent large unsigned values that wrapped around
            return 64;
        }

        return self::calculate($maxInt);
    }

    /**
     * @return array<int>
     */
    public static function toBytes(int $value, int $bitWidth) : array
    {
        $bytes = [];
        $width = (int) (($bitWidth + 7) / 8);

        for ($i = 0; $i < $width; $i++) {
            $bytes[] = ($value >> ($i * 8)) & 0xFF;
        }

        return $bytes;
    }
}
