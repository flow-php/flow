<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Inference;

use Flow\ETL\IO\Inference\Sample;
use Flow\ETL\IO\Inference\SampledUnits;
use Flow\ETL\IO\Inference\SniffedColumns;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Cardinality;

final class SampledUnitsTest extends FlowTestCase
{
    public function test_a_partial_sample_estimates_from_the_mean_row_size(): void
    {
        static::assertEquals(
            Cardinality::approximately(100),
            (new SampledUnits([new Sample(new SniffedColumns([], 10), 100, false)]))->estimatedRows(
                1,
                Cardinality::exact(1000),
            ),
        );
    }

    public function test_every_listed_unit_read_whole_is_exact(): void
    {
        $units = new SampledUnits([
            new Sample(new SniffedColumns([], 4), 40, true),
            new Sample(new SniffedColumns([], 6), 60, true),
        ]);

        static::assertEquals(Cardinality::exact(10), $units->estimatedRows(2, Cardinality::exact(100)));
        static::assertEquals(Cardinality::exact(10), $units->exactRows(2));
    }

    public function test_a_listed_unit_the_samples_never_read_makes_it_an_estimate(): void
    {
        $units = new SampledUnits([new Sample(new SniffedColumns([], 10), 100, true)]);

        static::assertEquals(Cardinality::approximately(20), $units->estimatedRows(2, Cardinality::exact(200)));
        static::assertEquals(Cardinality::unknown(), $units->exactRows(2));
    }

    public function test_an_incomplete_sample_is_never_exact(): void
    {
        static::assertEquals(
            Cardinality::unknown(),
            (new SampledUnits([new Sample(new SniffedColumns([], 10), 0, false)]))->exactRows(1),
        );
    }

    public function test_no_listed_bytes_or_no_sampled_bytes_is_unknown(): void
    {
        static::assertEquals(Cardinality::unknown(), (new SampledUnits([new Sample(
            new SniffedColumns([], 10),
            100,
            false,
        )]))->estimatedRows(1, Cardinality::unknown()));
        static::assertEquals(Cardinality::unknown(), (new SampledUnits([new Sample(
            new SniffedColumns([], 10),
            0,
            false,
        )]))->estimatedRows(1, Cardinality::exact(100)));
        static::assertEquals(Cardinality::unknown(), (new SampledUnits([new Sample(
            new SniffedColumns([], 0),
            0,
            false,
        )]))->estimatedRows(1, Cardinality::exact(100)));
    }

    public function test_no_samples_over_no_listed_units_is_exactly_nothing(): void
    {
        static::assertEquals(Cardinality::exact(0), (new SampledUnits([]))->exactRows(0));
        static::assertEquals(Cardinality::unknown(), (new SampledUnits([]))->exactRows(1));
    }
}
