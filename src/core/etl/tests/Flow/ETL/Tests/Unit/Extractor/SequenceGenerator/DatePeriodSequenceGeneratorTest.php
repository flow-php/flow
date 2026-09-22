<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor\SequenceGenerator;

use DateInterval;
use DatePeriod;
use DateTimeImmutable;
use Flow\ETL\Cardinality;
use Flow\ETL\Extractor\SequenceGenerator\DatePeriodSequenceGenerator;
use Flow\ETL\Tests\FlowTestCase;

use function iterator_count;

final class DatePeriodSequenceGeneratorTest extends FlowTestCase
{
    public function test_an_empty_period_declares_zero_rows(): void
    {
        $generator = new DatePeriodSequenceGenerator(
            new DatePeriod(
                new DateTimeImmutable('2023-01-10'),
                new DateInterval('P1D'),
                new DateTimeImmutable('2023-01-01'),
            ),
        );

        static::assertEquals(Cardinality::exact(0), $generator->rows());
    }

    public function test_excluding_the_start_date_is_counted(): void
    {
        $generator = new DatePeriodSequenceGenerator(
            new DatePeriod(
                new DateTimeImmutable('2023-01-01'),
                new DateInterval('P1D'),
                new DateTimeImmutable('2023-01-11'),
                DatePeriod::EXCLUDE_START_DATE,
            ),
        );

        static::assertEquals(Cardinality::exact(9), $generator->rows());
        static::assertSame(9, iterator_count($generator->generate()));
    }

    public function test_including_the_end_date_is_counted(): void
    {
        $generator = new DatePeriodSequenceGenerator(
            new DatePeriod(
                new DateTimeImmutable('2023-01-01'),
                new DateInterval('P1D'),
                new DateTimeImmutable('2023-01-11'),
                DatePeriod::INCLUDE_END_DATE,
            ),
        );

        static::assertEquals(Cardinality::exact(11), $generator->rows());
        static::assertSame(11, iterator_count($generator->generate()));
    }

    public function test_recurrences_are_counted(): void
    {
        $generator = new DatePeriodSequenceGenerator(
            new DatePeriod(new DateTimeImmutable('2023-01-01'), new DateInterval('P1D'), 5),
        );

        static::assertEquals(Cardinality::exact(6), $generator->rows());
        static::assertSame(6, iterator_count($generator->generate()));
    }

    public function test_variable_length_months_are_counted(): void
    {
        $generator = new DatePeriodSequenceGenerator(
            new DatePeriod(
                new DateTimeImmutable('2023-01-31'),
                new DateInterval('P1M'),
                new DateTimeImmutable('2023-12-31'),
            ),
        );

        static::assertEquals(Cardinality::exact(iterator_count($generator->generate())), $generator->rows());
    }
}
