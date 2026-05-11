<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\Schema\Diff\LongestCommonSubstringStrategy;
use PHPUnit\Framework\TestCase;

final class LongestCommonSubstringStrategyTest extends TestCase
{
    public function test_both_empty_strings_score_100(): void
    {
        static::assertSame(100.0, (new LongestCommonSubstringStrategy())->similarity('', ''));
    }

    public function test_common_prefix_with_different_suffix(): void
    {
        $strategy = new LongestCommonSubstringStrategy();

        $withSuffix = $strategy->similarity('user_email_old', 'user_email');
        $noOverlap = $strategy->similarity('user_email_old', 'total_amount');

        static::assertGreaterThan($noOverlap, $withSuffix);
    }

    public function test_custom_threshold(): void
    {
        static::assertSame(75.0, (new LongestCommonSubstringStrategy(75.0))->threshold());
    }

    public function test_default_threshold_is_50(): void
    {
        static::assertSame(50.0, (new LongestCommonSubstringStrategy())->threshold());
    }

    public function test_identical_names_score_100(): void
    {
        static::assertSame(100.0, (new LongestCommonSubstringStrategy())->similarity('column_name', 'column_name'));
    }

    public function test_no_common_characters_scores_0(): void
    {
        static::assertSame(0.0, (new LongestCommonSubstringStrategy())->similarity('abc', 'xyz'));
    }

    public function test_one_empty_string_scores_0(): void
    {
        static::assertSame(0.0, (new LongestCommonSubstringStrategy())->similarity('', 'something'));
    }

    public function test_realistic_column_rename_suffix_removed(): void
    {
        static::assertEqualsWithDelta(
            70.0,
            (new LongestCommonSubstringStrategy())->similarity('net_commission_cents', 'net_commission'),
            0.01,
        );
    }

    public function test_reversed_single_chars_share_one_character_substring(): void
    {
        static::assertSame(50.0, (new LongestCommonSubstringStrategy())->similarity('ax', 'xa'));
    }
}
