<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use Flow\Floe\Decoding\Float64Decoder;
use PHPUnit\Framework\TestCase;

use function pack;

final class Float64DecoderTest extends TestCase
{
    public function test_decodes_little_endian_double_and_advances_position(): void
    {
        $position = 0;

        static::assertSame(3.5, (new Float64Decoder())->decode(pack('e', 3.5), $position));
        static::assertSame(8, $position);
    }
}
