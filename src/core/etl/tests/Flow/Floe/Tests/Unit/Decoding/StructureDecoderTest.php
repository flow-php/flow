<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use Flow\Floe\Decoding\Int64Decoder;
use Flow\Floe\Decoding\StringDecoder;
use Flow\Floe\Decoding\StructureDecoder;
use Flow\Floe\Encoding\Int64Encoder;
use Flow\Floe\Encoding\StringEncoder;
use Flow\Floe\Encoding\StructureEncoder;
use Flow\Floe\Exception\FloeException;
use PHPUnit\Framework\TestCase;

final class StructureDecoderTest extends TestCase
{
    public function test_round_trip_with_present_null_and_absent_elements(): void
    {
        $encoder = new StructureEncoder([
            'id' => new Int64Encoder(),
            'name' => new StringEncoder(),
            'missing' => new Int64Encoder(),
        ]);
        $decoder = new StructureDecoder([
            'id' => new Int64Decoder(),
            'name' => new StringDecoder(),
            'missing' => new Int64Decoder(),
        ]);

        $position = 0;

        static::assertSame(
            ['id' => 1, 'name' => null],
            $decoder->decode($encoder->encode(['id' => 1, 'name' => null]), $position),
        );
    }

    public function test_unknown_element_flag_throws(): void
    {
        $decoder = new StructureDecoder(['name' => new StringDecoder()]);
        $position = 0;

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('unknown structure element flag');

        $decoder->decode("\xEE", $position);
    }
}
