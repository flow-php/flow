<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

use function count;
use function decbin;
use function max;
use function min;
use function strlen;

final class BitWidth
{
    /**
     * Packing through one int accumulator holds the value's bits plus up to 7 pending bits, which must stay below the
     * sign bit: 56 + 7 = 63.
     */
    public const int MAX_PACKED_IN_ACCUMULATOR = 56;

    public static function calculate(int $value): int
    {
        return $value === 0 ? 0 : strlen(decbin($value));
    }

    /**
     * @param array<int> $ints
     */
    public static function fromArray(array $ints): int
    {
        if (!count($ints)) {
            return 0;
        }

        $maxInt = max($ints);
        $minInt = min($ints);

        if ($maxInt === 0 && $minInt === 0) {
            return 0;
        }

        // a negative delta is an overflowed unsigned value, so it takes all 64 bits
        if ($minInt < 0) {
            return 64;
        }

        return self::calculate($maxInt);
    }

    /**
     * @return array<int>
     */
    public static function toBytes(int $value, int $bitWidth): array
    {
        $bytes = [];
        $width = (int) (($bitWidth + 7) / 8);

        for ($i = 0; $i < $width; $i++) {
            $bytes[] = ($value >> ($i * 8)) & 0xFF;
        }

        return $bytes;
    }
}
