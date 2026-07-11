<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use Flow\Floe\Decoding\JsonDecoder;
use Flow\Floe\Encoding\JsonEncoder;
use Flow\Types\Value\Json;
use PHPUnit\Framework\TestCase;

final class JsonDecoderTest extends TestCase
{
    public function test_round_trip_preserves_value_and_object_flag(): void
    {
        $decoder = new JsonDecoder();

        foreach ([new Json('[1,2,3]'), new Json('{"a":true}')] as $value) {
            $position = 0;
            $decoded = $decoder->decode((new JsonEncoder())->encode($value), $position);

            static::assertSame($value->toString(), $decoded->toString());
            static::assertSame($value->isObject(), $decoded->isObject());
        }
    }
}
