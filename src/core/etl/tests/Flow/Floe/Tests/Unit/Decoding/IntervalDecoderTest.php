<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Decoding;

use DateInterval;
use Flow\Floe\Decoding\IntervalDecoder;
use Flow\Floe\Encoding\IntervalEncoder;
use PHPUnit\Framework\TestCase;

final class IntervalDecoderTest extends TestCase
{
    public function test_round_trip_preserves_all_fields(): void
    {
        $value = new DateInterval('P1DT2H3M4S');
        // @mago-ignore analysis:invalid-property-write
        $value->invert = 1;
        // @mago-ignore analysis:invalid-property-write
        $value->f = 0.5;

        $position = 0;
        $decoded = (new IntervalDecoder())->decode((new IntervalEncoder())->encode($value), $position);

        static::assertSame([1, 2, 3, 4, 0.5, 1], [
            $decoded->d,
            $decoded->h,
            $decoded->i,
            $decoded->s,
            $decoded->f,
            $decoded->invert,
        ]);
    }
}
