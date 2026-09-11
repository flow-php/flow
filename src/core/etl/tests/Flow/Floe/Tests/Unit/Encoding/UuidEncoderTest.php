<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use Flow\Floe\Encoding\UuidEncoder;
use Flow\Types\Value\Uuid;
use PHPUnit\Framework\TestCase;

use function pack;
use function strlen;

final class UuidEncoderTest extends TestCase
{
    public function test_encodes_a_length_prefixed_canonical_string(): void
    {
        $uuid = '0196aecb-b568-7e57-a381-8ec8d3e4a531';

        static::assertSame(pack('V', strlen($uuid)) . $uuid, (new UuidEncoder())->encode(new Uuid($uuid)));
    }
}
