<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use Flow\Floe\Encoding\JsonEncoder;
use Flow\Types\Value\Json;
use PHPUnit\Framework\TestCase;

use function pack;

final class JsonEncoderTest extends TestCase
{
    public function test_encodes_length_prefixed_json_with_object_flag(): void
    {
        $encoder = new JsonEncoder();

        static::assertSame(pack('V', 7) . '[1,2,3]' . "\x00", $encoder->encode(new Json('[1,2,3]')));
        static::assertSame(pack('V', 10) . '{"a":true}' . "\x01", $encoder->encode(new Json('{"a":true}')));
    }
}
