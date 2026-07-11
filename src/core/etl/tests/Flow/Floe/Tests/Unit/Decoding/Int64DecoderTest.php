<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use Flow\Floe\Decoding\Int64Decoder;
use PHPUnit\Framework\TestCase;

use function pack;

use const PHP_INT_MAX;
use const PHP_INT_MIN;

final class Int64DecoderTest extends TestCase
{
    public function test_decodes_little_endian_signed_edges_and_advances_position(): void
    {
        $decoder = new Int64Decoder();

        foreach ([PHP_INT_MIN, PHP_INT_MAX, -5, 0, 42] as $value) {
            $position = 0;

            static::assertSame($value, $decoder->decode(pack('P', $value), $position));
            static::assertSame(8, $position);
        }
    }
}
