<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\Schema\Diff\NoSimilarityStrategy;
use PHPUnit\Framework\TestCase;

final class NoSimilarityStrategyTest extends TestCase
{
    public function test_always_returns_zero() : void
    {
        $strategy = new NoSimilarityStrategy();

        self::assertSame(0.0, $strategy->similarity('column_name', 'column_name'));
    }

    public function test_returns_zero_for_different_names() : void
    {
        $strategy = new NoSimilarityStrategy();

        self::assertSame(0.0, $strategy->similarity('foo', 'bar'));
    }

    public function test_returns_zero_for_empty_strings() : void
    {
        $strategy = new NoSimilarityStrategy();

        self::assertSame(0.0, $strategy->similarity('', ''));
    }

    public function test_threshold_is_100() : void
    {
        $strategy = new NoSimilarityStrategy();

        self::assertSame(100.0, $strategy->threshold());
    }
}
