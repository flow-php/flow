<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Time;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Time\Duration;
use PHPUnit\Framework\TestCase;

final class DurationTest extends TestCase
{
    public function test_conversion_precision() : void
    {
        $duration = Duration::fromMicroseconds(1_234_567);

        self::assertSame(1_234_567, $duration->microseconds());
        self::assertSame(1234, $duration->milliseconds());
        self::assertSame(1, $duration->seconds());
        self::assertSame(0, $duration->minutes());
    }

    public function test_create_from_microseconds() : void
    {
        $duration = Duration::fromMicroseconds(1000);

        self::assertSame(1000, $duration->microseconds());
        self::assertSame(1, $duration->milliseconds());
    }

    public function test_create_from_milliseconds() : void
    {
        $duration = Duration::fromMilliseconds(500);

        self::assertSame(500000, $duration->microseconds());
        self::assertSame(500, $duration->milliseconds());
        self::assertSame(0, $duration->seconds());
    }

    public function test_create_from_minutes() : void
    {
        $duration = Duration::fromMinutes(3);

        self::assertSame(180_000_000, $duration->microseconds());
        self::assertSame(180000, $duration->milliseconds());
        self::assertSame(180, $duration->seconds());
        self::assertSame(3, $duration->minutes());
    }

    public function test_create_from_seconds() : void
    {
        $duration = Duration::fromSeconds(2);

        self::assertSame(2_000_000, $duration->microseconds());
        self::assertSame(2000, $duration->milliseconds());
        self::assertSame(2, $duration->seconds());
        self::assertSame(0, $duration->minutes());
    }

    public function test_large_values() : void
    {
        $duration = Duration::fromMinutes(60);

        self::assertSame(3_600_000_000, $duration->microseconds());
        self::assertSame(3_600_000, $duration->milliseconds());
        self::assertSame(3600, $duration->seconds());
        self::assertSame(60, $duration->minutes());
    }

    public function test_negative_duration_throws_exception() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Duration cannot be negative');

        Duration::fromMicroseconds(-1);
    }

    public function test_negative_milliseconds_throws_exception() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Duration cannot be negative');

        Duration::fromMilliseconds(-100);
    }

    public function test_negative_minutes_throws_exception() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Duration cannot be negative');

        Duration::fromMinutes(-1);
    }

    public function test_negative_seconds_throws_exception() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Duration cannot be negative');

        Duration::fromSeconds(-1);
    }

    public function test_zero_duration() : void
    {
        $duration = Duration::fromMicroseconds(0);

        self::assertSame(0, $duration->microseconds());
        self::assertSame(0, $duration->milliseconds());
        self::assertSame(0, $duration->seconds());
        self::assertSame(0, $duration->minutes());
    }
}
