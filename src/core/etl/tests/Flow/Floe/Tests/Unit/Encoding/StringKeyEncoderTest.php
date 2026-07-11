<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use Flow\Floe\Encoding\StringKeyEncoder;
use PHPUnit\Framework\TestCase;

use function pack;

final class StringKeyEncoderTest extends TestCase
{
    public function test_encodes_string_key(): void
    {
        static::assertSame(pack('V', 3) . 'abc', (new StringKeyEncoder())->encode('abc'));
    }

    public function test_encodes_numeric_string_key_arriving_as_integer(): void
    {
        // PHP turns numeric-string array keys into integers
        static::assertSame(pack('V', 3) . '123', (new StringKeyEncoder())->encode(123));
    }
}
