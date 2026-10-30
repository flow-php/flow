<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\File;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\File\FooterStatistics;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Cardinality;

final class FooterStatisticsTest extends FlowTestCase
{
    public function test_every_listed_footer_read_is_exact(): void
    {
        static::assertEquals(
            new Statistics(rows: Cardinality::exact(30), size: Cardinality::exact(300)),
            (new FooterStatistics(new Statistics(Cardinality::exact(30), Cardinality::exact(300)), 3))->of(3),
        );
    }

    public function test_the_first_footer_is_scaled_to_the_listing(): void
    {
        static::assertEquals(
            new Statistics(rows: Cardinality::approximately(40), size: Cardinality::approximately(400)),
            (new FooterStatistics(new Statistics(Cardinality::exact(10), Cardinality::exact(100)), 1))->of(4),
        );
    }

    public function test_no_footer_read_is_zero_exactly(): void
    {
        static::assertEquals(
            new Statistics(rows: Cardinality::exact(0), size: Cardinality::exact(0)),
            (new FooterStatistics(new Statistics(Cardinality::exact(0), Cardinality::exact(0)), 0))->of(0),
        );
    }

    public function test_a_negative_footer_count_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Read footer count must not be negative, given: -1');

        new FooterStatistics(new Statistics(), -1);
    }
}
