<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\MessengerMetricDurationUnit;
use Flow\Telemetry\Meter\Instrument\Histogram;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(MessengerMetricDurationUnit::class)]
final class MessengerMetricDurationUnitTest extends TestCase
{
    public function test_value_maps_to_otel_unit_string(): void
    {
        static::assertSame('s', MessengerMetricDurationUnit::Seconds->value);
        static::assertSame('ms', MessengerMetricDurationUnit::Milliseconds->value);
    }

    public function test_seconds_uses_otel_bucket_boundaries(): void
    {
        static::assertSame(
            [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0],
            MessengerMetricDurationUnit::Seconds->histogramBoundaries(),
        );
    }

    public function test_milliseconds_uses_default_buckets(): void
    {
        static::assertNull(MessengerMetricDurationUnit::Milliseconds->histogramBoundaries());
    }

    public function test_from_milliseconds_converts_per_unit(): void
    {
        static::assertSame(1.5, MessengerMetricDurationUnit::Seconds->fromMilliseconds(1500.0));
        static::assertSame(1500.0, MessengerMetricDurationUnit::Milliseconds->fromMilliseconds(1500.0));
    }

    public function test_null_boundaries_fall_back_to_histogram_default(): void
    {
        static::assertNotEmpty(Histogram::DEFAULT_BOUNDARIES);
    }
}
