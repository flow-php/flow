<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Dataset\Statistics;

use Flow\ETL\Dataset\Statistics\HighResolutionTime;
use Flow\ETL\Tests\FlowTestCase;

final class HighResolutionTimeTest extends FlowTestCase
{
    public function test_constructor_initializes_correctly() : void
    {
        $time = new HighResolutionTime(123, 456789000);

        self::assertSame([123, 456789000], $time->toArray());
        self::assertEquals(123.456789, $time->toSeconds());
    }

    public function test_diff_when_nanoseconds_need_adjustment() : void
    {
        $t1 = new HighResolutionTime(10, 500000000); // 10.5s
        $t2 = new HighResolutionTime(10, 300000000); // 10.3s

        $diff = $t1->diff($t2);

        // 10.3s - 10.5s = -0.2s => [-1, 800000000]
        self::assertSame([-1, 800000000], $diff->toArray());
        self::assertEqualsWithDelta(-0.2, $diff->toSeconds(), 1e-9);
    }

    public function test_diff_with_no_difference() : void
    {
        $t1 = new HighResolutionTime(10, 500000000);
        $diff = $t1->diff($t1);

        self::assertSame([0, 0], $diff->toArray());
        self::assertEquals(0.0, $diff->toSeconds());
    }

    public function test_diff_with_positive_difference() : void
    {
        $t1 = new HighResolutionTime(10, 500000000); // 10.5s
        $t2 = new HighResolutionTime(12, 100000000); // 12.1s

        $diff = $t1->diff($t2);

        // We expect 1.6s difference
        self::assertSame([1, 600000000], $diff->toArray());
        self::assertEqualsWithDelta(1.6, $diff->toSeconds(), 1e-9);
    }

    public function test_now_creates_current_time_instance() : void
    {
        $time = HighResolutionTime::now();
        self::assertInstanceOf(HighResolutionTime::class, $time);
    }

    public function test_to_formatted_string_removes_trailing_zeroes() : void
    {
        // 5.250000000 => should display as "5.25"
        $time = new HighResolutionTime(5, 250000000);
        self::assertSame('5.25s', $time->toString());

        // 0.001000000 => should display as "0.001"
        $time = new HighResolutionTime(0, 1000000); // 1,000,000ns = 0.001s
        self::assertSame('0.001s', $time->toString());

        // 3.000000000 => should display as "3"
        $time = new HighResolutionTime(3, 0);
        self::assertSame('3s', $time->toString());
    }
}
