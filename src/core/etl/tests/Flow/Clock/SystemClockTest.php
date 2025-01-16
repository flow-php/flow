<?php

declare(strict_types=1);

namespace Flow\Clock;

use PHPUnit\Framework\TestCase;

final class SystemClockTest extends TestCase
{
    public function test_now() : void
    {
        $clock = SystemClock::system();

        self::assertInstanceOf(\DateTimeImmutable::class, $clock->now());
    }

    public function test_system_clock() : void
    {
        $clock = SystemClock::system();

        self::assertInstanceOf(SystemClock::class, $clock);
    }

    public function test_utc_clock() : void
    {
        $clock = SystemClock::utc();

        self::assertInstanceOf(SystemClock::class, $clock);
    }
}
