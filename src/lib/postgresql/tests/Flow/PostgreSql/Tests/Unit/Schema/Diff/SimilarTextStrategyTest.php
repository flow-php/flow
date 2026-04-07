<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\Schema\Diff\SimilarTextStrategy;
use PHPUnit\Framework\TestCase;

final class SimilarTextStrategyTest extends TestCase
{
    public function test_custom_threshold() : void
    {
        self::assertSame(75.0, (new SimilarTextStrategy(75.0))->threshold());
    }

    public function test_default_threshold_is_50() : void
    {
        self::assertSame(50.0, (new SimilarTextStrategy())->threshold());
    }

    public function test_empty_strings_score_0() : void
    {
        self::assertSame(0.0, (new SimilarTextStrategy())->similarity('', ''));
    }

    public function test_identical_names_score_100() : void
    {
        self::assertSame(100.0, (new SimilarTextStrategy())->similarity('column_name', 'column_name'));
    }

    public function test_partial_overlap_scores_between_50_and_100() : void
    {
        self::assertEqualsWithDelta(66.67, (new SimilarTextStrategy())->similarity('last_name', 'full_name'), 0.01);
    }

    public function test_realistic_column_rename_scores_above_80() : void
    {
        self::assertEqualsWithDelta(82.35, (new SimilarTextStrategy())->similarity('net_commission_cents', 'net_commission'), 0.01);
    }

    public function test_strategies_rank_correctly_for_ambiguous_commission_columns() : void
    {
        $strategy = new SimilarTextStrategy();

        $bestMatch = $strategy->similarity('net_commission_cents', 'net_commission');
        $crossMatch = $strategy->similarity('net_commission_cents', 'total_commission');

        self::assertGreaterThan($crossMatch, $bestMatch);
    }

    public function test_unrelated_names_score_below_threshold() : void
    {
        self::assertEqualsWithDelta(7.41, (new SimilarTextStrategy())->similarity('foo', 'xyz_completely_different'), 0.01);
    }
}
