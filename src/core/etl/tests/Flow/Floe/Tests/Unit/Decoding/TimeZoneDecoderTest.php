<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use Flow\Floe\Decoding\TimeZoneDecoder;
use Flow\Floe\Decoding\TimeZones;
use PHPUnit\Framework\TestCase;

use function pack;

final class TimeZoneDecoderTest extends TestCase
{
    public function test_decodes_length_prefixed_timezone_name(): void
    {
        $decoder = new TimeZoneDecoder(new TimeZones());
        $position = 0;

        static::assertSame(
            'Australia/Eucla',
            $decoder->decode(pack('V', 15) . 'Australia/Eucla', $position)->getName(),
        );
        static::assertSame(19, $position);
    }
}
