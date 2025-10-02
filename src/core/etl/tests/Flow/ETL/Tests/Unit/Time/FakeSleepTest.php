<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Time;

use Flow\ETL\Time\{Duration, FakeSleep};
use PHPUnit\Framework\TestCase;

final class FakeSleepTest extends TestCase
{
    public function test_no_actual_delay_occurs() : void
    {
        $sleep = new FakeSleep();

        $startTime = \microtime(true);
        $sleep->for(Duration::fromSeconds(10));
        $endTime = \microtime(true);

        $actualElapsed = $endTime - $startTime;

        self::assertLessThan(0.1, $actualElapsed);
    }

    public function test_records_all_sleep_durations() : void
    {
        $sleep = new FakeSleep();

        $duration1 = Duration::fromMilliseconds(100);
        $duration2 = Duration::fromSeconds(2);
        $duration3 = Duration::fromMinutes(1);

        $sleep->for($duration1);
        $sleep->for($duration2);
        $sleep->for($duration3);

        $sleepDurations = $sleep->sleepDurations();

        self::assertCount(3, $sleepDurations);
        self::assertSame(100, $sleepDurations[0]->milliseconds());
        self::assertSame(2000, $sleepDurations[1]->milliseconds());
        self::assertSame(60000, $sleepDurations[2]->milliseconds());
    }

    public function test_reset() : void
    {
        $sleep = new FakeSleep();

        $sleep->for(Duration::fromSeconds(5));
        $sleep->for(Duration::fromSeconds(10));

        self::assertSame(15_000_000, $sleep->totalMicroseconds());
        self::assertCount(2, $sleep->sleepDurations());
        self::assertSame(2, $sleep->sleepCount());

        $sleep->reset();

        self::assertSame(0, $sleep->totalMicroseconds());
        self::assertSame(0, $sleep->totalMilliseconds());
        self::assertSame(0, $sleep->totalSeconds());
        self::assertCount(0, $sleep->sleepDurations());
        self::assertSame(0, $sleep->sleepCount());
    }

    public function test_sleep_count() : void
    {
        $sleep = new FakeSleep();

        self::assertSame(0, $sleep->sleepCount());

        $sleep->for(Duration::fromMilliseconds(100));
        self::assertSame(1, $sleep->sleepCount());

        $sleep->for(Duration::fromMilliseconds(200));
        self::assertSame(2, $sleep->sleepCount());

        $sleep->for(Duration::fromMilliseconds(300));
        self::assertSame(3, $sleep->sleepCount());
    }

    public function test_tracks_total_sleep_time() : void
    {
        $sleep = new FakeSleep();

        $sleep->for(Duration::fromMilliseconds(100));
        $sleep->for(Duration::fromMilliseconds(200));
        $sleep->for(Duration::fromSeconds(1));

        self::assertSame(1_300_000, $sleep->totalMicroseconds());
        self::assertSame(1300, $sleep->totalMilliseconds());
        self::assertSame(1, $sleep->totalSeconds());
    }
}
