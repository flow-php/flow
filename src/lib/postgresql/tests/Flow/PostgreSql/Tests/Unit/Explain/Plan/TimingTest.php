<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Explain\Plan;

use Flow\PostgreSql\Explain\Plan\Timing;
use PHPUnit\Framework\TestCase;

final class TimingTest extends TestCase
{
    public function test_from_array_and_normalize_are_inverse(): void
    {
        $original = new Timing(startupTime: 0.5, totalTime: 25.75, loops: 3);

        $normalized = $original->normalize();
        $restored = Timing::fromArray($normalized);

        static::assertEquals($original, $restored);
    }

    public function test_from_array_creates_instance(): void
    {
        $data = [
            'startup_time' => 0.5,
            'total_time' => 25.75,
            'loops' => 3,
        ];

        $timing = Timing::fromArray($data);

        static::assertSame(0.5, $timing->startupTime());
        static::assertSame(25.75, $timing->totalTime());
        static::assertSame(3, $timing->loops());
    }

    public function test_normalize_returns_all_fields(): void
    {
        $timing = new Timing(startupTime: 0.5, totalTime: 25.75, loops: 3);

        $normalized = $timing->normalize();

        static::assertSame(0.5, $normalized['startup_time']);
        static::assertSame(25.75, $normalized['total_time']);
        static::assertSame(3, $normalized['loops']);
    }

    public function test_normalize_returns_expected_keys(): void
    {
        $timing = new Timing(startupTime: 0.0, totalTime: 100.0, loops: 1);

        $normalized = $timing->normalize();

        $expectedKeys = [
            'startup_time',
            'total_time',
            'loops',
        ];

        static::assertSame($expectedKeys, \array_keys($normalized));
    }
}
