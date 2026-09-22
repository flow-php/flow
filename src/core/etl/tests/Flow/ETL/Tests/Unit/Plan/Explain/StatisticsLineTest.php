<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Explain;

use Flow\ETL\Cardinality;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\Plan\Explain\StatisticsLine;
use Flow\ETL\Tests\FlowTestCase;

use const INF;

final class StatisticsLineTest extends FlowTestCase
{
    public function test_an_exact_count_is_written_as_exact(): void
    {
        static::assertSame('exact 1 000', (new StatisticsLine())->cardinality(Cardinality::exact(1_000)));
    }

    public function test_an_upper_bound_alone_is_written_as_at_most(): void
    {
        static::assertSame('≤ 25 000', (new StatisticsLine())->cardinality(Cardinality::atMost(25_000)));
    }

    public function test_an_estimate_alone_carries_its_relative_error_in_percent(): void
    {
        static::assertSame('~20 000 ±50%', (new StatisticsLine())->cardinality(Cardinality::approximately(20_000)));
        static::assertSame(
            '~20 000 ±12.5%',
            (new StatisticsLine())->cardinality(Cardinality::approximately(20_000, 0.125)),
        );
    }

    public function test_an_estimate_with_an_error_of_zero_and_no_bound_is_not_exact(): void
    {
        static::assertSame('~7 ±0%', (new StatisticsLine())->cardinality(Cardinality::approximately(7, 0.0)));
    }

    public function test_an_estimate_under_a_bound_lists_both(): void
    {
        static::assertSame(
            '~800 ±25% (≤ 1 000)',
            (new StatisticsLine())->cardinality(new Cardinality(1_000, 800, 0.25)),
        );
    }

    public function test_an_estimate_equal_to_its_bound_is_not_exact_while_it_carries_an_error(): void
    {
        static::assertSame('~1 000 ±50% (≤ 1 000)', (new StatisticsLine())->cardinality(new Cardinality(1_000, 1_000)));
    }

    public function test_an_unknown_count_is_written_as_unknown(): void
    {
        static::assertSame('unknown', (new StatisticsLine())->cardinality(Cardinality::unknown()));
    }

    public function test_numbers_are_grouped_in_thousands(): void
    {
        static::assertSame('exact 0', (new StatisticsLine())->cardinality(Cardinality::exact(0)));
        static::assertSame('exact 999', (new StatisticsLine())->cardinality(Cardinality::exact(999)));
        static::assertSame(
            'exact 1 234 567 890',
            (new StatisticsLine())->cardinality(Cardinality::exact(1_234_567_890)),
        );
    }

    public function test_the_unit_follows_every_number(): void
    {
        static::assertSame('~800 B ±25% (≤ 1 000 B)', (new StatisticsLine())->cardinality(
            new Cardinality(1_000, 800, 0.25),
            ' B',
        ));
        static::assertSame('≤ 1 000 B', (new StatisticsLine())->cardinality(Cardinality::atMost(1_000), ' B'));
        static::assertSame('exact 309 188 B', (new StatisticsLine())->cardinality(Cardinality::exact(309_188), ' B'));
        static::assertSame('unknown', (new StatisticsLine())->cardinality(Cardinality::unknown(), ' B'));
    }

    public function test_a_relative_error_is_written_in_percent(): void
    {
        static::assertSame('0%', (new StatisticsLine())->percent(0.0));
        static::assertSame('45%', (new StatisticsLine())->percent(0.45));
        static::assertSame('33.33%', (new StatisticsLine())->percent(1 / 3));
        static::assertSame('250%', (new StatisticsLine())->percent(2.5));
    }

    public function test_an_infinite_relative_error_is_written_as_infinity(): void
    {
        static::assertSame('∞', (new StatisticsLine())->percent(INF));
    }

    public function test_a_missing_relative_error_is_written_as_unknown(): void
    {
        static::assertSame('unknown', (new StatisticsLine())->percent(null));
    }

    public function test_the_line_lists_rows_then_size_in_bytes(): void
    {
        static::assertSame(
            'rows exact 1 000 · size exact 309 188 B',
            (new StatisticsLine())->of(new Statistics(Cardinality::exact(1_000), Cardinality::exact(309_188))),
        );
    }

    public function test_the_line_lists_an_unknown_fact_next_to_a_known_one(): void
    {
        static::assertSame(
            'rows unknown · size exact 10 B',
            (new StatisticsLine())->of(new Statistics(size: Cardinality::exact(10))),
        );
        static::assertSame(
            'rows ≤ 5 · size unknown',
            (new StatisticsLine())->of(new Statistics(rows: Cardinality::atMost(5))),
        );
    }

    public function test_there_is_no_line_when_both_facts_are_unknown(): void
    {
        static::assertNull((new StatisticsLine())->of(new Statistics()));
    }
}
