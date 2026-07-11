<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use Flow\Floe\Encoding\NullEncoder;
use PHPUnit\Framework\TestCase;

final class NullEncoderTest extends TestCase
{
    public function test_encodes_nothing(): void
    {
        static::assertSame('', (new NullEncoder())->encode(null));
    }
}
