<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use Flow\Floe\Decoding\BooleanDecoder;
use PHPUnit\Framework\TestCase;

final class BooleanDecoderTest extends TestCase
{
    public function test_decodes_single_byte_and_advances_position(): void
    {
        $decoder = new BooleanDecoder();
        $position = 0;

        static::assertTrue($decoder->decode("\x01\x00", $position));
        static::assertFalse($decoder->decode("\x01\x00", $position));
        static::assertSame(2, $position);
    }
}
