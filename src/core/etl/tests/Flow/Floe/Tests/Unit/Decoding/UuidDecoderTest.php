<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use Flow\Floe\Decoding\UuidDecoder;
use Flow\Types\Value\Uuid;
use PHPUnit\Framework\TestCase;

use function pack;
use function strlen;

final class UuidDecoderTest extends TestCase
{
    public function test_decodes_a_length_prefixed_value_without_validation(): void
    {
        $uuid = '0196aecb-b568-7e57-a381-8ec8d3e4a531';
        $decoder = new UuidDecoder();
        $position = 0;

        $decoded = $decoder->decode(pack('V', strlen($uuid)) . $uuid, $position);

        static::assertInstanceOf(Uuid::class, $decoded);
        static::assertSame($uuid, $decoded->toString());
        static::assertSame(4 + strlen($uuid), $position);
    }
}
