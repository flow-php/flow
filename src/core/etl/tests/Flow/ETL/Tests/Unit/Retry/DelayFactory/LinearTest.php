<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Retry\DelayFactory;

use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\delay_linear;
use function Flow\ETL\DSL\duration_microseconds;
use function Flow\ETL\DSL\duration_milliseconds;
use function Flow\ETL\DSL\duration_seconds;

final class LinearTest extends TestCase
{
    public function test_first_attempt_equals_base_duration(): void
    {
        $delayFactory = delay_linear(duration_milliseconds(750), duration_seconds(1));

        static::assertSame(750_000, $delayFactory->delay(1)->microseconds());
    }

    public function test_linear_backoff(): void
    {
        $delayFactory = delay_linear(duration_seconds(1), duration_milliseconds(500));

        static::assertSame(1_000_000, $delayFactory->delay(1)->microseconds());
        static::assertSame(1_500_000, $delayFactory->delay(2)->microseconds());
        static::assertSame(2_000_000, $delayFactory->delay(3)->microseconds());
        static::assertSame(2_500_000, $delayFactory->delay(4)->microseconds());
    }

    public function test_linear_backoff_with_different_increments(): void
    {
        $delayFactory = delay_linear(duration_milliseconds(100), duration_milliseconds(100));

        static::assertSame(100_000, $delayFactory->delay(1)->microseconds());
        static::assertSame(200_000, $delayFactory->delay(2)->microseconds());
        static::assertSame(300_000, $delayFactory->delay(3)->microseconds());
        static::assertSame(400_000, $delayFactory->delay(4)->microseconds());
    }

    public function test_linear_backoff_with_large_attempt_numbers(): void
    {
        $delayFactory = delay_linear(duration_milliseconds(10), duration_milliseconds(5));

        static::assertSame(505_000, $delayFactory->delay(100)->microseconds());
    }

    public function test_linear_backoff_with_zero_increment(): void
    {
        $delayFactory = delay_linear(duration_seconds(2), duration_microseconds(0));

        static::assertSame(2_000_000, $delayFactory->delay(1)->microseconds());
        static::assertSame(2_000_000, $delayFactory->delay(2)->microseconds());
        static::assertSame(2_000_000, $delayFactory->delay(10)->microseconds());
    }
}
