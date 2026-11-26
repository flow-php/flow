<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use DateInterval;

final class DateIntervalMother
{
    /**
     * @param int<0, 1> $invert
     */
    public static function of(string $spec, float $fraction, int $invert): DateInterval
    {
        $interval = new DateInterval($spec);
        $interval->f = $fraction;
        $interval->invert = $invert;

        return $interval;
    }

    /**
     * @param int<0, 1> $invert
     */
    public static function seconds(int $seconds, float $fraction, int $invert): DateInterval
    {
        $interval = self::of('PT0S', $fraction, $invert);
        $interval->s = $seconds;

        return $interval;
    }
}
