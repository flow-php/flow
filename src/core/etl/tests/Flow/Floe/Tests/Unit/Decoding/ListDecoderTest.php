<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use Flow\Floe\Decoding\ListDecoder;
use Flow\Floe\Decoding\StringDecoder;
use Flow\Floe\Encoding\ListEncoder;
use Flow\Floe\Encoding\StringEncoder;
use PHPUnit\Framework\TestCase;

final class ListDecoderTest extends TestCase
{
    public function test_round_trip_of_count_prefixed_elements(): void
    {
        $decoder = new ListDecoder(new StringDecoder());
        $position = 0;

        static::assertSame(
            ['a', 'bc'],
            $decoder->decode((new ListEncoder(new StringEncoder()))->encode(['a', 'bc']), $position),
        );
    }
}
