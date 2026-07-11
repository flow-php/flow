<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use Flow\Floe\Encoding\Int64Encoder;
use Flow\Floe\Encoding\MapEncoder;
use Flow\Floe\Encoding\StringEncoder;
use Flow\Floe\Encoding\StringKeyEncoder;
use PHPUnit\Framework\TestCase;

use function pack;

final class MapEncoderTest extends TestCase
{
    public function test_encodes_count_prefixed_key_value_pairs(): void
    {
        $encoder = new MapEncoder(new StringKeyEncoder(), new Int64Encoder());

        static::assertSame(pack('V', 1) . pack('V', 1) . 'a' . pack('P', 42), $encoder->encode(['a' => 42]));
    }

    public function test_encodes_integer_keys(): void
    {
        $encoder = new MapEncoder(new Int64Encoder(), new StringEncoder());

        static::assertSame(pack('V', 1) . pack('P', 7) . pack('V', 1) . 'x', $encoder->encode([7 => 'x']));
    }
}
