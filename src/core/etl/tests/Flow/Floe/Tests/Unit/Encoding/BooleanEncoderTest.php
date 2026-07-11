<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use Flow\Floe\Encoding\BooleanEncoder;
use PHPUnit\Framework\TestCase;

final class BooleanEncoderTest extends TestCase
{
    public function test_encodes_single_byte(): void
    {
        $encoder = new BooleanEncoder();

        static::assertSame("\x01", $encoder->encode(true));
        static::assertSame("\x00", $encoder->encode(false));
    }
}
