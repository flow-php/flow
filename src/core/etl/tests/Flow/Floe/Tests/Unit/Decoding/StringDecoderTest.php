<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use Flow\Floe\Decoding\StringDecoder;
use PHPUnit\Framework\TestCase;

use function pack;

final class StringDecoderTest extends TestCase
{
    public function test_decodes_length_prefixed_bytes_and_advances_position(): void
    {
        $decoder = new StringDecoder();
        $position = 0;

        static::assertSame('abc', $decoder->decode(pack('V', 3) . 'abc', $position));
        static::assertSame(7, $position);

        $position = 0;
        static::assertSame('', $decoder->decode(pack('V', 0), $position));
        static::assertSame(4, $position);
    }
}
