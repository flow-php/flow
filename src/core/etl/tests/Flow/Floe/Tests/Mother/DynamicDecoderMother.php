<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Mother;

use Flow\Floe\Decoding\DateTimeDecoder;
use Flow\Floe\Decoding\DynamicDecoder;
use Flow\Floe\Decoding\JsonDecoder;
use Flow\Floe\Decoding\TimeZones;
use Flow\Floe\Decoding\UuidDecoder;

final class DynamicDecoderMother
{
    public static function create(): DynamicDecoder
    {
        return new DynamicDecoder(new DateTimeDecoder(new TimeZones()), new UuidDecoder(), new JsonDecoder());
    }
}
