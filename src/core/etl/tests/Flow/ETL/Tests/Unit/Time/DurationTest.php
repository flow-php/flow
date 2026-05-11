<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Time;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Time\Duration;
use PHPUnit\Framework\TestCase;

final class DurationTest extends TestCase
{
    public function test_conversion_precision(): void
    {
        $duration = Duration::fromMicroseconds(1_234_567);

        static::assertSame(1_234_567, $duration->microseconds());
        static::assertSame(1234, $duration->milliseconds());
        static::assertSame(1, $duration->seconds());
        static::assertSame(0, $duration->minutes());
    }

    public function test_create_from_microseconds(): void
    {
        $duration = Duration::fromMicroseconds(1000);

        static::assertSame(1000, $duration->microseconds());
        static::assertSame(1, $duration->milliseconds());
    }

    public function test_create_from_milliseconds(): void
    {
        $duration = Duration::fromMilliseconds(500);

        static::assertSame(500000, $duration->microseconds());
        static::assertSame(500, $duration->milliseconds());
        static::assertSame(0, $duration->seconds());
    }

    public function test_create_from_minutes(): void
    {
        $duration = Duration::fromMinutes(3);

        static::assertSame(180_000_000, $duration->microseconds());
        static::assertSame(180000, $duration->milliseconds());
        static::assertSame(180, $duration->seconds());
        static::assertSame(3, $duration->minutes());
    }

    public function test_create_from_seconds(): void
    {
        $duration = Duration::fromSeconds(2);

        static::assertSame(2_000_000, $duration->microseconds());
        static::assertSame(2000, $duration->milliseconds());
        static::assertSame(2, $duration->seconds());
        static::assertSame(0, $duration->minutes());
    }

    public function test_large_values(): void
    {
        $duration = Duration::fromMinutes(60);

        static::assertSame(3_600_000_000, $duration->microseconds());
        static::assertSame(3_600_000, $duration->milliseconds());
        static::assertSame(3600, $duration->seconds());
        static::assertSame(60, $duration->minutes());
    }

    public function test_negative_duration_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Duration cannot be negative');

        Duration::fromMicroseconds(-1);
    }

    public function test_negative_milliseconds_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Duration cannot be negative');

        Duration::fromMilliseconds(-100);
    }

    public function test_negative_minutes_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Duration cannot be negative');

        Duration::fromMinutes(-1);
    }

    public function test_negative_seconds_throws_exception(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Duration cannot be negative');

        Duration::fromSeconds(-1);
    }

    public function test_zero_duration(): void
    {
        $duration = Duration::fromMicroseconds(0);

        static::assertSame(0, $duration->microseconds());
        static::assertSame(0, $duration->milliseconds());
        static::assertSame(0, $duration->seconds());
        static::assertSame(0, $duration->minutes());
    }
}
