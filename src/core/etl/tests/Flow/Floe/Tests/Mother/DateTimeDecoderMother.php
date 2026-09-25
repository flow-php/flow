<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Mother;

use DateTimeZone;
use Flow\Floe\Decoding\DateTimeDecoder;
use Flow\Floe\Decoding\TimeZones;

final class DateTimeDecoderMother
{
    public static function create(): DateTimeDecoder
    {
        return new DateTimeDecoder(new TimeZones());
    }

    public static function inColumnZone(string $zone): DateTimeDecoder
    {
        return new DateTimeDecoder(new TimeZones(), new DateTimeZone($zone));
    }
}
