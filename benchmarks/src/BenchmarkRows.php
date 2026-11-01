<?php

declare(strict_types=1);

namespace Flow\Benchmarks;

use function getenv;

/**
 * Providers and materialise.php must agree; phpbench --parameters replaces rather than merges a parameter set.
 */
final class BenchmarkRows
{
    public const DEFAULT = 100_000;

    public const WINDOW_DEFAULT = 10_000;

    public static function count(): int
    {
        return (int) (getenv('FLOW_BENCH_ROWS') ?: self::DEFAULT);
    }

    /**
     * The orders dataset spreads rows over 5 sellers, so every row lands in a partition of rows/5.
     * Frame-materializing aggregates are quadratic in partition size, hence a lower default.
     */
    public static function windowCount(): int
    {
        return (int) (getenv('FLOW_BENCH_WINDOW_ROWS') ?: self::WINDOW_DEFAULT);
    }
}
