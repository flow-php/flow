<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Time;

use Flow\ETL\Time\Duration;
use Flow\ETL\Time\FakeSleep;
use PHPUnit\Framework\TestCase;

final class FakeSleepTest extends TestCase
{
    public function test_no_actual_delay_occurs(): void
    {
        $sleep = new FakeSleep();

        $startTime = \microtime(true);
        $sleep->for(Duration::fromSeconds(10));
        $endTime = \microtime(true);

        $actualElapsed = $endTime - $startTime;

        static::assertLessThan(0.1, $actualElapsed);
    }

    public function test_records_all_sleep_durations(): void
    {
        $sleep = new FakeSleep();

        $duration1 = Duration::fromMilliseconds(100);
        $duration2 = Duration::fromSeconds(2);
        $duration3 = Duration::fromMinutes(1);

        $sleep->for($duration1);
        $sleep->for($duration2);
        $sleep->for($duration3);

        $sleepDurations = $sleep->sleepDurations();

        static::assertCount(3, $sleepDurations);
        static::assertSame(100, $sleepDurations[0]->milliseconds());
        static::assertSame(2000, $sleepDurations[1]->milliseconds());
        static::assertSame(60000, $sleepDurations[2]->milliseconds());
    }

    public function test_reset(): void
    {
        $sleep = new FakeSleep();

        $sleep->for(Duration::fromSeconds(5));
        $sleep->for(Duration::fromSeconds(10));

        static::assertSame(15_000_000, $sleep->totalMicroseconds());
        static::assertCount(2, $sleep->sleepDurations());
        static::assertSame(2, $sleep->sleepCount());

        $sleep->reset();

        static::assertSame(0, $sleep->totalMicroseconds());
        static::assertSame(0, $sleep->totalMilliseconds());
        static::assertSame(0, $sleep->totalSeconds());
        static::assertCount(0, $sleep->sleepDurations());
        static::assertSame(0, $sleep->sleepCount());
    }

    public function test_sleep_count(): void
    {
        $sleep = new FakeSleep();

        static::assertSame(0, $sleep->sleepCount());

        $sleep->for(Duration::fromMilliseconds(100));
        static::assertSame(1, $sleep->sleepCount());

        $sleep->for(Duration::fromMilliseconds(200));
        static::assertSame(2, $sleep->sleepCount());

        $sleep->for(Duration::fromMilliseconds(300));
        static::assertSame(3, $sleep->sleepCount());
    }

    public function test_tracks_total_sleep_time(): void
    {
        $sleep = new FakeSleep();

        $sleep->for(Duration::fromMilliseconds(100));
        $sleep->for(Duration::fromMilliseconds(200));
        $sleep->for(Duration::fromSeconds(1));

        static::assertSame(1_300_000, $sleep->totalMicroseconds());
        static::assertSame(1300, $sleep->totalMilliseconds());
        static::assertSame(1, $sleep->totalSeconds());
    }
}
