<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use Flow\Floe\Encoding\ListEncoder;
use Flow\Floe\Encoding\StringEncoder;
use PHPUnit\Framework\TestCase;

use function pack;

final class ListEncoderTest extends TestCase
{
    public function test_encodes_count_prefixed_elements(): void
    {
        $encoder = new ListEncoder(new StringEncoder());

        static::assertSame(pack('V', 2) . pack('V', 1) . 'a' . pack('V', 2) . 'bc', $encoder->encode(['a', 'bc']));
        static::assertSame(pack('V', 0), $encoder->encode([]));
    }
}
