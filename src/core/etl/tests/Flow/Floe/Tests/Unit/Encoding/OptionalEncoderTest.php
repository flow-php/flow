<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use Flow\Floe\Encoding\Int64Encoder;
use Flow\Floe\Encoding\OptionalEncoder;
use Flow\Floe\Format;
use PHPUnit\Framework\TestCase;

use function pack;

final class OptionalEncoderTest extends TestCase
{
    public function test_encodes_null_flag_or_present_value(): void
    {
        $encoder = new OptionalEncoder(new Int64Encoder());

        static::assertSame(Format::VALUE_NULL_BYTE, $encoder->encode(null));
        static::assertSame(Format::VALUE_PRESENT_BYTE . pack('P', 42), $encoder->encode(42));
    }
}
