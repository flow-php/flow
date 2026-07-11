<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use Flow\Floe\Decoding\NullDecoder;
use PHPUnit\Framework\TestCase;

final class NullDecoderTest extends TestCase
{
    public function test_decodes_null_without_consuming_bytes(): void
    {
        $position = 0;

        static::assertNull((new NullDecoder())->decode('', $position));
        static::assertSame(0, $position);
    }
}
