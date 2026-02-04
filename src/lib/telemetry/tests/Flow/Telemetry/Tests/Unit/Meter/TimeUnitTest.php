<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter;

use Flow\Telemetry\Meter\TimeUnit;
use PHPUnit\Framework\TestCase;

final class TimeUnitTest extends TestCase
{
    public function test_converts_nanoseconds_to_microseconds() : void
    {
        self::assertEqualsWithDelta(1_000.0, TimeUnit::MICROSECONDS->fromNanoseconds(1_000_000), 0.001);
    }

    public function test_converts_nanoseconds_to_milliseconds() : void
    {
        self::assertEqualsWithDelta(1_000.0, TimeUnit::MILLISECONDS->fromNanoseconds(1_000_000_000), 0.001);
    }

    public function test_converts_nanoseconds_to_minutes() : void
    {
        self::assertEqualsWithDelta(1.0, TimeUnit::MINUTES->fromNanoseconds(60_000_000_000), 0.001);
    }

    public function test_converts_nanoseconds_to_seconds() : void
    {
        self::assertEqualsWithDelta(2.5, TimeUnit::SECONDS->fromNanoseconds(2_500_000_000), 0.001);
    }

    public function test_has_correct_string_values() : void
    {
        self::assertSame('ns', TimeUnit::NANOSECONDS->value);
        self::assertSame('µs', TimeUnit::MICROSECONDS->value);
        self::assertSame('ms', TimeUnit::MILLISECONDS->value);
        self::assertSame('sec', TimeUnit::SECONDS->value);
        self::assertSame('min', TimeUnit::MINUTES->value);
    }

    public function test_returns_nanoseconds_as_float() : void
    {
        self::assertSame(1_000_000.0, TimeUnit::NANOSECONDS->fromNanoseconds(1_000_000));
    }
}
