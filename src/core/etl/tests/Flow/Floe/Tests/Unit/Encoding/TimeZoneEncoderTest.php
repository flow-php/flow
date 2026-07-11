<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use DateTimeZone;
use Flow\Floe\Encoding\TimeZoneEncoder;
use PHPUnit\Framework\TestCase;

use function pack;

final class TimeZoneEncoderTest extends TestCase
{
    public function test_encodes_length_prefixed_timezone_name(): void
    {
        static::assertSame(
            pack('V', 15) . 'Australia/Eucla',
            (new TimeZoneEncoder())->encode(new DateTimeZone('Australia/Eucla')),
        );
    }
}
