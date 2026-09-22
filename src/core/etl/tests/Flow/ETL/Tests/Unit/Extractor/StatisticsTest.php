<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Cardinality;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\Tests\FlowTestCase;

final class StatisticsTest extends FlowTestCase
{
    public function test_a_source_that_knows_nothing_declares_unknown_for_both_facts(): void
    {
        $statistics = new Statistics();

        static::assertEquals(Cardinality::unknown(), $statistics->rows);
        static::assertEquals(Cardinality::unknown(), $statistics->size);
    }

    public function test_merge_adds_rows_and_size_independently(): void
    {
        static::assertEquals(
            new Statistics(Cardinality::exact(30), Cardinality::approximately(300)),
            (new Statistics(Cardinality::exact(10), Cardinality::approximately(100)))->merge(
                new Statistics(Cardinality::exact(20), Cardinality::exact(200)),
            ),
        );
    }

    public function test_merging_with_an_unknown_partial_yields_unknown(): void
    {
        static::assertEquals(
            new Statistics(),
            (new Statistics(Cardinality::exact(10), Cardinality::exact(100)))->merge(new Statistics()),
        );
    }

    public function test_rows_and_size_are_declared_independently(): void
    {
        $statistics = new Statistics(size: Cardinality::exact(1024));

        static::assertEquals(Cardinality::unknown(), $statistics->rows);
        static::assertEquals(Cardinality::exact(1024), $statistics->size);
    }
}
