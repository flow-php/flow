<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Cardinality;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\FooterStatistics;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\Tests\FlowTestCase;

final class FooterStatisticsTest extends FlowTestCase
{
    public function test_every_listed_footer_read_is_exact(): void
    {
        static::assertEquals(
            new Statistics(rows: Cardinality::exact(30), size: Cardinality::exact(300)),
            (new FooterStatistics(new Statistics(Cardinality::exact(30), Cardinality::exact(300)), 3))->of(3, 0),
        );
    }

    public function test_the_first_footer_is_scaled_to_the_listing(): void
    {
        static::assertEquals(
            new Statistics(rows: Cardinality::approximately(40), size: Cardinality::approximately(400)),
            (new FooterStatistics(new Statistics(Cardinality::exact(10), Cardinality::exact(100)), 1))->of(4, 0),
        );
    }

    public function test_the_offset_is_taken_off_the_rows_never_below_zero(): void
    {
        $footers = new FooterStatistics(new Statistics(Cardinality::exact(10), Cardinality::exact(100)), 1);

        static::assertEquals(Cardinality::exact(7), $footers->of(1, 3)->rows);
        static::assertEquals(Cardinality::exact(0), $footers->of(1, 30)->rows);
        static::assertEquals(Cardinality::approximately(37), $footers->of(4, 3)->rows);
    }

    public function test_no_footer_read_is_zero_exactly(): void
    {
        static::assertEquals(
            new Statistics(rows: Cardinality::exact(0), size: Cardinality::exact(0)),
            (new FooterStatistics(new Statistics(Cardinality::exact(0), Cardinality::exact(0)), 0))->of(0, 0),
        );
    }

    public function test_a_negative_footer_count_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Read footer count must not be negative, given: -1');

        new FooterStatistics(new Statistics(), -1);
    }
}
