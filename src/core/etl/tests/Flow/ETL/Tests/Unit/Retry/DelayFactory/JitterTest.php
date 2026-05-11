<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Retry\DelayFactory;

use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\delay_exponential;
use function Flow\ETL\DSL\delay_fixed;
use function Flow\ETL\DSL\delay_jitter;
use function Flow\ETL\DSL\duration_microseconds;
use function Flow\ETL\DSL\duration_seconds;

final class JitterTest extends TestCase
{
    public function test_invalid_jitter_percentage_above_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Jitter percentage must be between 0.0 and 1.0');

        delay_jitter(delay_fixed(duration_seconds(1)), 1.1);
    }

    public function test_invalid_jitter_percentage_below_zero(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Jitter percentage must be between 0.0 and 1.0');

        delay_jitter(delay_fixed(duration_seconds(1)), -0.1);
    }

    public function test_jitter_adds_variation(): void
    {
        $jitterFactory = delay_jitter(delay_fixed(duration_seconds(1)), 0.5);

        $delays = [];

        for ($i = 0; $i < 100; $i++) {
            $delays[] = $jitterFactory->delay(1)->microseconds();
        }

        $minExpected = 500_000;
        $maxExpected = 1_500_000;

        $minActual = \min($delays);
        $maxActual = \max($delays);

        static::assertNotSame($minActual, $maxActual, 'Jitter should create variation in delays');

        foreach ($delays as $delay) {
            static::assertGreaterThanOrEqual($minExpected, $delay);
            static::assertLessThanOrEqual($maxExpected, $delay);
        }
    }

    public function test_jitter_boundaries(): void
    {
        $jitterFactory = delay_jitter(delay_fixed(duration_seconds(10)), 1.0);

        $delays = [];

        for ($i = 0; $i < 1000; $i++) {
            $delays[] = $jitterFactory->delay(1)->microseconds();
        }

        $minDelay = \min($delays);
        $maxDelay = \max($delays);

        static::assertGreaterThanOrEqual(0, $minDelay);
        static::assertLessThanOrEqual(20_000_000, $maxDelay);
    }

    public function test_jitter_never_goes_negative(): void
    {
        $jitterFactory = delay_jitter(delay_fixed(duration_microseconds(100)), 1.0);

        for ($i = 0; $i < 100; $i++) {
            $delay = $jitterFactory->delay(1);
            static::assertGreaterThanOrEqual(0, $delay->microseconds());
        }
    }

    public function test_jitter_with_different_delay_factories(): void
    {
        $jitterFactory = delay_jitter(delay_exponential(duration_seconds(1)), 0.2);

        $attempt1Delay = $jitterFactory->delay(1);
        $attempt2Delay = $jitterFactory->delay(2);

        static::assertGreaterThanOrEqual(800_000, $attempt1Delay->microseconds());
        static::assertLessThanOrEqual(1_200_000, $attempt1Delay->microseconds());

        static::assertGreaterThanOrEqual(1_600_000, $attempt2Delay->microseconds());
        static::assertLessThanOrEqual(2_400_000, $attempt2Delay->microseconds());
    }

    public function test_jitter_with_zero_percentage(): void
    {
        $jitterFactory = delay_jitter(delay_fixed(duration_seconds(1)), 0.0);

        static::assertSame(1_000_000, $jitterFactory->delay(1)->microseconds());
        static::assertSame(1_000_000, $jitterFactory->delay(2)->microseconds());
    }
}
