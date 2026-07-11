<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use Flow\Floe\Decoding\Int64Decoder;
use Flow\Floe\Decoding\MapDecoder;
use Flow\Floe\Decoding\StringDecoder;
use Flow\Floe\Encoding\Int64Encoder;
use Flow\Floe\Encoding\MapEncoder;
use Flow\Floe\Encoding\StringEncoder;
use Flow\Floe\Encoding\StringKeyEncoder;
use PHPUnit\Framework\TestCase;

final class MapDecoderTest extends TestCase
{
    public function test_round_trip_of_string_keyed_map(): void
    {
        $decoder = new MapDecoder(new StringDecoder(), new Int64Decoder());
        $position = 0;

        static::assertSame(
            ['a' => 1, 'b' => 2],
            $decoder->decode((new MapEncoder(new StringKeyEncoder(), new Int64Encoder()))->encode([
                'a' => 1,
                'b' => 2,
            ]), $position),
        );
    }

    public function test_round_trip_of_integer_keyed_map(): void
    {
        $decoder = new MapDecoder(new Int64Decoder(), new StringDecoder());
        $position = 0;

        static::assertSame(
            [7 => 'x'],
            $decoder->decode((new MapEncoder(new Int64Encoder(), new StringEncoder()))->encode([7 => 'x']), $position),
        );
    }
}
