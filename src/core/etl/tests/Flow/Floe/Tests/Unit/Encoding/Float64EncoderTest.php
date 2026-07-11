<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use Flow\Floe\Encoding\Float64Encoder;
use PHPUnit\Framework\TestCase;

use function pack;

final class Float64EncoderTest extends TestCase
{
    public function test_encodes_little_endian_double(): void
    {
        $encoder = new Float64Encoder();

        static::assertSame(pack('e', 3.5), $encoder->encode(3.5));
        static::assertSame(pack('e', -0.25), $encoder->encode(-0.25));
    }
}
