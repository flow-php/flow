<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Retry\DelayFactory;

use function Flow\ETL\DSL\{delay_fixed, duration_milliseconds, duration_seconds};
use PHPUnit\Framework\TestCase;

final class FixedTest extends TestCase
{
    public function test_returns_correct_duration_for_different_base_durations() : void
    {
        $millisecondDelay = delay_fixed(duration_milliseconds(500));
        $secondDelay = delay_fixed(duration_seconds(2));

        self::assertSame(500_000, $millisecondDelay->delay(1)->microseconds());
        self::assertSame(2_000_000, $secondDelay->delay(1)->microseconds());
    }

    public function test_returns_same_duration_for_all_attempts() : void
    {
        $duration = duration_seconds(1);
        $delayFactory = delay_fixed($duration);

        self::assertSame(1_000_000, $delayFactory->delay(1)->microseconds());
        self::assertSame(1_000_000, $delayFactory->delay(2)->microseconds());
        self::assertSame(1_000_000, $delayFactory->delay(10)->microseconds());
        self::assertSame(1_000_000, $delayFactory->delay(100)->microseconds());
    }
}
