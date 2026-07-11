<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use Flow\Floe\Decoding\UuidDecoder;
use Flow\Types\Value\Uuid;
use PHPUnit\Framework\TestCase;

final class UuidDecoderTest extends TestCase
{
    public function test_decodes_36_bytes_without_validation(): void
    {
        $decoder = new UuidDecoder();
        $position = 0;

        $decoded = $decoder->decode('0196aecb-b568-7e57-a381-8ec8d3e4a531', $position);

        static::assertInstanceOf(Uuid::class, $decoded);
        static::assertSame('0196aecb-b568-7e57-a381-8ec8d3e4a531', $decoded->toString());
        static::assertSame(36, $position);
    }
}
