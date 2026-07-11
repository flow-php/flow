<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use Flow\Floe\Encoding\UuidEncoder;
use Flow\Types\Value\Uuid;
use PHPUnit\Framework\TestCase;

final class UuidEncoderTest extends TestCase
{
    public function test_encodes_canonical_36_byte_string(): void
    {
        static::assertSame(
            '0196aecb-b568-7e57-a381-8ec8d3e4a531',
            (new UuidEncoder())->encode(new Uuid('0196aecb-b568-7e57-a381-8ec8d3e4a531')),
        );
    }
}
