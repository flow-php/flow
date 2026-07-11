<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use Flow\Floe\Decoding\PackedListDecoder;
use Flow\Floe\Encoding\PackedListEncoder;
use PHPUnit\Framework\TestCase;

final class PackedListDecoderTest extends TestCase
{
    public function test_round_trip_of_int64_list(): void
    {
        $position = 0;

        static::assertSame(
            [10, -20, 30],
            (new PackedListDecoder('P'))->decode((new PackedListEncoder('P'))->encode([10, -20, 30]), $position),
        );
    }

    public function test_round_trip_of_float64_list(): void
    {
        $position = 0;

        static::assertSame(
            [1.5, -2.5],
            (new PackedListDecoder('e'))->decode((new PackedListEncoder('e'))->encode([1.5, -2.5]), $position),
        );
    }

    public function test_empty_list(): void
    {
        $position = 0;

        static::assertSame(
            [],
            (new PackedListDecoder('P'))->decode((new PackedListEncoder('P'))->encode([]), $position),
        );
        static::assertSame(4, $position);
    }
}
