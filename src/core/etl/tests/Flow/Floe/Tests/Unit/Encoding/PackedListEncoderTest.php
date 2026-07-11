<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use Flow\Floe\Encoding\PackedListEncoder;
use PHPUnit\Framework\TestCase;

use function pack;

final class PackedListEncoderTest extends TestCase
{
    public function test_encodes_int64_list_in_bulk(): void
    {
        $encoder = new PackedListEncoder('P');

        static::assertSame(pack('V', 3) . pack('P*', 10, -20, 30), $encoder->encode([10, -20, 30]));
        static::assertSame(pack('V', 0), $encoder->encode([]));
    }

    public function test_encodes_float64_list_in_bulk(): void
    {
        $encoder = new PackedListEncoder('e');

        static::assertSame(pack('V', 2) . pack('e*', 1.5, -2.5), $encoder->encode([1.5, -2.5]));
    }
}
