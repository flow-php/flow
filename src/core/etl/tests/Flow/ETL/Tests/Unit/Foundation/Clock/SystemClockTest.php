<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Foundation\Clock;

use DateTimeImmutable;
use Flow\ETL\Foundation\Clock\SystemClock;
use PHPUnit\Framework\TestCase;

final class SystemClockTest extends TestCase
{
    public function test_now(): void
    {
        static::assertInstanceOf(DateTimeImmutable::class, SystemClock::system()->now());
    }

    public function test_system_clock(): void
    {
        static::assertInstanceOf(SystemClock::class, SystemClock::system());
    }

    public function test_utc_clock(): void
    {
        static::assertInstanceOf(SystemClock::class, SystemClock::utc());
    }
}
