<?php

declare(strict_types=1);

namespace Flow\Benchmarks;

use function getenv;

/**
 * The row counts every bench provider and materialise.php resolve through. They must agree: a fixture
 * set materialised for a different count leaves the first timed run generating one.
 *
 * phpbench's own --parameters cannot carry this - it replaces a subject's whole parameter set rather
 * than merging into it, so any subject with a second provider would lose that axis.
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
