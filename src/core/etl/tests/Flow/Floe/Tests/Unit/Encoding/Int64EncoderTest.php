<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use Flow\Floe\Encoding\Int64Encoder;
use PHPUnit\Framework\TestCase;

use function pack;

use const PHP_INT_MAX;
use const PHP_INT_MIN;

final class Int64EncoderTest extends TestCase
{
    public function test_encodes_little_endian_signed_edges(): void
    {
        $encoder = new Int64Encoder();

        static::assertSame(pack('P', PHP_INT_MIN), $encoder->encode(PHP_INT_MIN));
        static::assertSame(pack('P', PHP_INT_MAX), $encoder->encode(PHP_INT_MAX));
        static::assertSame("\xFB\xFF\xFF\xFF\xFF\xFF\xFF\xFF", $encoder->encode(-5));
        static::assertSame("\x2A\x00\x00\x00\x00\x00\x00\x00", $encoder->encode(42));
    }
}
