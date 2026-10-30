<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Column\Frame;

use Flow\ETL\Values\Column\Frame\NoopCodec;
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
