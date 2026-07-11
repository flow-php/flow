<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Mother;

use Flow\Floe\Decoding\DateTimeDecoder;
use Flow\Floe\Decoding\TimeZones;

final class DateTimeDecoderMother
{
    public static function create(): DateTimeDecoder
    {
        return new DateTimeDecoder(new TimeZones());
    }
}
