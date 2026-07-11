<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use Flow\Floe\Decoding\Int64Decoder;
use Flow\Floe\Decoding\StringDecoder;
use Flow\Floe\Decoding\StructureDecoder;
use Flow\Floe\Encoding\DynamicEncoder;
use Flow\Floe\Encoding\Int64Encoder;
use Flow\Floe\Encoding\StringEncoder;
use Flow\Floe\Encoding\StructureEncoder;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\Tests\Mother\DynamicDecoderMother;
use PHPUnit\Framework\TestCase;

final class StructureDecoderTest extends TestCase
{
    public function test_round_trip_with_present_null_and_absent_elements(): void
    {
        $encoder = new StructureEncoder(
            ['id' => new Int64Encoder(), 'name' => new StringEncoder(), 'missing' => new Int64Encoder()],
            false,
            new DynamicEncoder(),
        );
        $decoder = new StructureDecoder(
            ['id' => new Int64Decoder(), 'name' => new StringDecoder(), 'missing' => new Int64Decoder()],
            false,
            DynamicDecoderMother::create(),
        );

        $position = 0;

        static::assertSame(
            ['id' => 1, 'name' => null],
            $decoder->decode($encoder->encode(['id' => 1, 'name' => null]), $position),
        );
    }

    public function test_round_trip_with_extras(): void
    {
        $encoder = new StructureEncoder(['id' => new Int64Encoder()], true, new DynamicEncoder());
        $decoder = new StructureDecoder(['id' => new Int64Decoder()], true, DynamicDecoderMother::create());

        $position = 0;

        static::assertSame(
            ['id' => 1, 'extra' => 2],
            $decoder->decode($encoder->encode(['id' => 1, 'extra' => 2]), $position),
        );
    }

    public function test_unknown_element_flag_throws(): void
    {
        $decoder = new StructureDecoder(['name' => new StringDecoder()], false, DynamicDecoderMother::create());
        $position = 0;

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('unknown structure element flag');

        $decoder->decode("\xEE", $position);
    }
}
