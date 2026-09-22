<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Dataset;

use Flow\ETL\Cardinality;
use Flow\ETL\Dataset\SourceStatistics;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use const INF;

final class SourceStatisticsTest extends FlowTestCase
{
    #[TestWith([20_000, 29_000, 9_000 / 29_000])]
    #[TestWith([30_000, 20_000, 0.5])]
    #[TestWith([100, 100, 0.0])]
    public function test_the_rows_error_is_the_estimates_distance_relative_to_the_rows_measured(
        int $estimate,
        int $measured,
        float $error,
    ): void {
        static::assertEqualsWithDelta(
            $error,
            (new SourceStatistics(
                'Source',
                new Statistics(Cardinality::approximately($estimate)),
                $measured,
            ))->rowsError(),
            0.000_001,
        );
    }

    public function test_an_exact_declaration_that_holds_has_no_error(): void
    {
        static::assertSame(
            0.0,
            (new SourceStatistics('Source', new Statistics(Cardinality::exact(5)), 5))->rowsError(),
        );
    }

    public function test_there_is_no_error_without_declared_rows(): void
    {
        static::assertNull((new SourceStatistics('Source', new Statistics(), 5))->rowsError());
    }

    public function test_an_upper_bound_alone_has_no_error(): void
    {
        static::assertNull((new SourceStatistics('Source', new Statistics(Cardinality::atMost(10)), 5))->rowsError());
    }

    public function test_an_estimate_of_zero_against_zero_rows_has_no_error(): void
    {
        static::assertSame(
            0.0,
            (new SourceStatistics('Source', new Statistics(Cardinality::approximately(0)), 0))->rowsError(),
        );
    }

    public function test_a_positive_estimate_against_zero_rows_is_infinitely_off(): void
    {
        static::assertSame(
            INF,
            (new SourceStatistics('Source', new Statistics(Cardinality::approximately(500)), 0))->rowsError(),
        );
    }
}
