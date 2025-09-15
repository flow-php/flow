<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Retry\DelayFactory;

use function Flow\ETL\DSL\delay_exponential;
use Flow\ETL\Time\Duration;
use PHPUnit\Framework\TestCase;

final class ExponentialTest extends TestCase
{
    public function test_exponential_backoff_with_custom_multiplier() : void
    {
        $baseDuration = Duration::fromSeconds(1);
        $delayFactory = delay_exponential($baseDuration, 3);

        self::assertSame(1_000_000, $delayFactory->delay(1)->microseconds());
        self::assertSame(3_000_000, $delayFactory->delay(2)->microseconds());
        self::assertSame(9_000_000, $delayFactory->delay(3)->microseconds());
        self::assertSame(27_000_000, $delayFactory->delay(4)->microseconds());
    }

    public function test_exponential_backoff_with_default_multiplier() : void
    {
        $baseDuration = Duration::fromSeconds(1);
        $delayFactory = delay_exponential($baseDuration);

        self::assertSame(1_000_000, $delayFactory->delay(1)->microseconds());
        self::assertSame(2_000_000, $delayFactory->delay(2)->microseconds());
        self::assertSame(4_000_000, $delayFactory->delay(3)->microseconds());
        self::assertSame(8_000_000, $delayFactory->delay(4)->microseconds());
    }

    public function test_exponential_backoff_with_large_attempt_numbers() : void
    {
        $baseDuration = Duration::fromMilliseconds(100);
        $maxDelay = Duration::fromSeconds(60);
        $delayFactory = delay_exponential($baseDuration, 2, $maxDelay);

        $delay = $delayFactory->delay(20);
        self::assertSame(60_000_000, $delay->microseconds());
    }

    public function test_exponential_backoff_with_max_delay() : void
    {
        $baseDuration = Duration::fromSeconds(1);
        $maxDelay = Duration::fromSeconds(5);
        $delayFactory = delay_exponential($baseDuration, 2, $maxDelay);

        self::assertSame(1_000_000, $delayFactory->delay(1)->microseconds());
        self::assertSame(2_000_000, $delayFactory->delay(2)->microseconds());
        self::assertSame(4_000_000, $delayFactory->delay(3)->microseconds());
        self::assertSame(5_000_000, $delayFactory->delay(4)->microseconds());
        self::assertSame(5_000_000, $delayFactory->delay(5)->microseconds());
    }

    public function test_first_attempt_equals_base_duration() : void
    {
        $baseDuration = Duration::fromMilliseconds(250);
        $delayFactory = delay_exponential($baseDuration);

        self::assertSame(250_000, $delayFactory->delay(1)->microseconds());
    }
}
