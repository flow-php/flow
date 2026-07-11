<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\Floe\FrameSegment;
use PHPUnit\Framework\TestCase;

final class FrameSegmentTest extends TestCase
{
    public function test_exposes_a_new_section_segment(): void
    {
        $segment = new FrameSegment('[{"ref":"id"}]', "\x02\x03\x00\x00\x00abc", 1);

        static::assertSame('[{"ref":"id"}]', $segment->schemaBody);
        static::assertSame("\x02\x03\x00\x00\x00abc", $segment->frames);
        static::assertSame(1, $segment->rowCount);
    }

    public function test_exposes_a_continuation_segment(): void
    {
        $segment = new FrameSegment(null, '', 0);

        static::assertNull($segment->schemaBody);
        static::assertSame('', $segment->frames);
        static::assertSame(0, $segment->rowCount);
    }
}
