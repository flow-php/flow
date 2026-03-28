<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\Schema\Diff\LevenshteinStrategy;
use PHPUnit\Framework\TestCase;

final class LevenshteinStrategyTest extends TestCase
{
    public function test_both_empty_strings_score_100() : void
    {
        self::assertSame(100.0, (new LevenshteinStrategy())->similarity('', ''));
    }

    public function test_custom_threshold() : void
    {
        self::assertSame(75.0, (new LevenshteinStrategy(75.0))->threshold());
    }

    public function test_default_threshold_is_50() : void
    {
        self::assertSame(50.0, (new LevenshteinStrategy())->threshold());
    }

    public function test_identical_names_score_100() : void
    {
        self::assertSame(100.0, (new LevenshteinStrategy())->similarity('column_name', 'column_name'));
    }

    public function test_levenshtein_penalizes_suffix_removal_more_than_similar_text() : void
    {
        $levenshtein = new LevenshteinStrategy();

        $score = $levenshtein->similarity('net_commission_cents', 'net_commission');

        self::assertEqualsWithDelta(70.0, $score, 0.01);
        self::assertLessThan(80.0, $score);
    }

    public function test_one_empty_string_scores_0() : void
    {
        self::assertSame(0.0, (new LevenshteinStrategy())->similarity('', 'something'));
    }

    public function test_realistic_column_rename_suffix_removed() : void
    {
        self::assertEqualsWithDelta(70.0, (new LevenshteinStrategy())->similarity('net_commission_cents', 'net_commission'), 0.01);
    }

    public function test_single_character_typo_scores_above_90() : void
    {
        self::assertEqualsWithDelta(90.91, (new LevenshteinStrategy())->similarity('column_name', 'column_namx'), 0.01);
    }

    public function test_unrelated_names_score_low() : void
    {
        $score = (new LevenshteinStrategy())->similarity('foo', 'xyz_completely_different');

        self::assertLessThan(20.0, $score);
    }
}
