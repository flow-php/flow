<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit\Encoding;

use DateInterval;
use Flow\Floe\Encoding\IntervalEncoder;
use PHPUnit\Framework\TestCase;

use function pack;

final class IntervalEncoderTest extends TestCase
{
    public function test_encodes_invert_fields_and_fraction(): void
    {
        $value = new DateInterval('P1Y2M3DT4H5M6S');
        // @mago-ignore analysis:invalid-property-write
        $value->invert = 1;
        // @mago-ignore analysis:invalid-property-write
        $value->f = 0.5;

        static::assertSame(
            "\x01" . pack('VVVVVV', 1, 2, 3, 4, 5, 6) . pack('e', 0.5),
            (new IntervalEncoder())->encode($value),
        );
    }
}
