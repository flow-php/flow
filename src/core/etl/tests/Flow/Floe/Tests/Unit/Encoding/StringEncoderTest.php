<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use Flow\Floe\Encoding\StringEncoder;
use PHPUnit\Framework\TestCase;

use function pack;

final class StringEncoderTest extends TestCase
{
    public function test_encodes_length_prefixed_bytes(): void
    {
        $encoder = new StringEncoder();

        static::assertSame(pack('V', 3) . 'abc', $encoder->encode('abc'));
        static::assertSame(pack('V', 0), $encoder->encode(''));
    }
}
