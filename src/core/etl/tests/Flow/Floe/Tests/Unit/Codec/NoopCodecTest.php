<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Codec;

use Flow\Floe\Codec\NoopCodec;
use PHPUnit\Framework\TestCase;

final class NoopCodecTest extends TestCase
{
    public function test_decode_is_passthrough(): void
    {
        static::assertSame("raw\x00bytes", (new NoopCodec())->decode("raw\x00bytes"));
    }

    public function test_encode_is_passthrough(): void
    {
        static::assertSame("raw\x00bytes", (new NoopCodec())->encode("raw\x00bytes"));
    }

    public function test_id_is_zero(): void
    {
        static::assertSame(0x00, (new NoopCodec())->id());
    }
}
