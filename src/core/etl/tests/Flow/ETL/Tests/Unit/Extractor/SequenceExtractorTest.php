<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use function Flow\ETL\DSL\{date_entry,
    float_entry,
    from_sequence_date_period,
    from_sequence_date_period_recurrences,
    from_sequence_number};
use function Flow\ETL\DSL\{row, rows};
use Flow\ETL\{Tests\FlowTestCase};

final class SequenceExtractorTest extends FlowTestCase
{
    public function test_extracting_from_date_period() : void
    {
        $extractor = from_sequence_date_period('day', new \DateTimeImmutable('2023-01-01'), new \DateInterval('P1D'), new \DateTimeImmutable('2023-01-11'), \DatePeriod::EXCLUDE_START_DATE);

        self::assertExtractedRowsEquals(
            rows(row(date_entry('day', new \DateTimeImmutable('2023-01-02'))), row(date_entry('day', new \DateTimeImmutable('2023-01-03'))), row(date_entry('day', new \DateTimeImmutable('2023-01-04'))), row(date_entry('day', new \DateTimeImmutable('2023-01-05'))), row(date_entry('day', new \DateTimeImmutable('2023-01-06'))), row(date_entry('day', new \DateTimeImmutable('2023-01-07'))), row(date_entry('day', new \DateTimeImmutable('2023-01-08'))), row(date_entry('day', new \DateTimeImmutable('2023-01-09'))), row(date_entry('day', new \DateTimeImmutable('2023-01-10')))),
            $extractor
        );
    }

    public function test_extracting_from_date_period_recurrences() : void
    {
        $extractor = from_sequence_date_period_recurrences('day', new \DateTimeImmutable('2023-01-01'), new \DateInterval('P1D'), 10, \DatePeriod::EXCLUDE_START_DATE);

        self::assertExtractedRowsEquals(
            rows(row(date_entry('day', new \DateTimeImmutable('2023-01-02'))), row(date_entry('day', new \DateTimeImmutable('2023-01-03'))), row(date_entry('day', new \DateTimeImmutable('2023-01-04'))), row(date_entry('day', new \DateTimeImmutable('2023-01-05'))), row(date_entry('day', new \DateTimeImmutable('2023-01-06'))), row(date_entry('day', new \DateTimeImmutable('2023-01-07'))), row(date_entry('day', new \DateTimeImmutable('2023-01-08'))), row(date_entry('day', new \DateTimeImmutable('2023-01-09'))), row(date_entry('day', new \DateTimeImmutable('2023-01-10'))), row(date_entry('day', new \DateTimeImmutable('2023-01-11')))),
            $extractor
        );
    }

    public function test_extracting_from_numbers_range() : void
    {
        $extractor = from_sequence_number('num', 0, 10, 1.5);

        self::assertExtractedRowsEquals(
            rows(row(float_entry('num', 0)), row(float_entry('num', 1.5)), row(float_entry('num', 3)), row(float_entry('num', 4.5)), row(float_entry('num', 6)), row(float_entry('num', 7.5)), row(float_entry('num', 9))),
            $extractor
        );
    }
}
