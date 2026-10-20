<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Mother;

use Flow\Parquet\Filter\Evaluation\ZoneMap;

use const NAN;

final class ZoneMapMother
{
    /**
     * Every value that is not null is NaN.
     */
    public static function allNaN(int $nullCount = 0): ZoneMap
    {
        return new ZoneMap(NAN, NAN, true, true, $nullCount, 10 - $nullCount, 10, true);
    }

    public static function allNull(int $rows = 10): ZoneMap
    {
        return new ZoneMap(null, null, true, true, $rows, null, $rows, false);
    }

    /**
     * No null and, for a float column, no NaN.
     */
    public static function clean(bool|float|int|string $min, bool|float|int|string $max): ZoneMap
    {
        return new ZoneMap($min, $max, true, true, 0, 0, 10, false);
    }

    public static function inexact(string $min, string $max): ZoneMap
    {
        return new ZoneMap($min, $max, false, false, 0, 0, 10, false);
    }

    public static function ieee(float $min, float $max, int $nullCount = 0, ?int $nanCount = null): ZoneMap
    {
        return new ZoneMap($min, $max, true, true, $nullCount, $nanCount, 10, false);
    }

    public static function unknownNaN(float $min, float $max): ZoneMap
    {
        return new ZoneMap($min, $max, true, true, 0, null, 10, false);
    }

    public static function unknownNulls(bool|float|int|string $min, bool|float|int|string $max): ZoneMap
    {
        return new ZoneMap($min, $max, true, true, null, 0, 10, false);
    }

    public static function untrusted(int $nullCount = 0): ZoneMap
    {
        return new ZoneMap(null, null, true, true, $nullCount, null, 10, false);
    }

    public static function withNulls(
        bool|float|int|string $min,
        bool|float|int|string $max,
        int $nullCount = 3,
    ): ZoneMap {
        return new ZoneMap($min, $max, true, true, $nullCount, 0, 10, false);
    }
}
