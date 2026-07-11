<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use Flow\Floe\Decoding\Int64Decoder;
use Flow\Floe\Decoding\OptionalDecoder;
use Flow\Floe\Encoding\Int64Encoder;
use Flow\Floe\Encoding\OptionalEncoder;
use PHPUnit\Framework\TestCase;

final class OptionalDecoderTest extends TestCase
{
    public function test_round_trip_of_null_and_present_values(): void
    {
        $encoder = new OptionalEncoder(new Int64Encoder());
        $decoder = new OptionalDecoder(new Int64Decoder());

        foreach ([null, 42] as $value) {
            $position = 0;

            static::assertSame($value, $decoder->decode($encoder->encode($value), $position));
        }
    }
}
