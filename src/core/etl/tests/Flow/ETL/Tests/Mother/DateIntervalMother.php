<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use DateInterval;

final class DateIntervalMother
{
    public static function of(string $spec, float $fraction, int $invert): DateInterval
    {
        $interval = new DateInterval($spec);
        // @mago-ignore analysis:invalid-property-write
        $interval->f = $fraction;
        // @mago-ignore analysis:invalid-property-write
        $interval->invert = $invert;

        return $interval;
    }

    public static function seconds(int $seconds, float $fraction, int $invert): DateInterval
    {
        $interval = self::of('PT0S', $fraction, $invert);
        // @mago-ignore analysis:invalid-property-write
        $interval->s = $seconds;

        return $interval;
    }
}
