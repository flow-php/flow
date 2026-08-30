<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use DateInterval;
use DatePeriod;
use DateTimeImmutable;
use Flow\ETL\Extractor\SequenceExtractor;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Tests\Double\MixedSequenceGenerator;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_sequence_date_period;
use function Flow\ETL\DSL\from_sequence_date_period_recurrences;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class SequenceExtractorTest extends FlowTestCase
{
    public function test_declared_schema_is_used_to_hydrate_extracted_rows(): void
    {
        self::assertExtractedRowsEquals(
            rows(schema(str_schema('num')), row(['num' => '1']), row(['num' => '2']), row(['num' => '3'])),
            from_sequence_number('num', 1, 3)->withSchema(schema(str_schema('num'))),
        );
    }

    public function test_extracting_from_date_period(): void
    {
        $extractor = from_sequence_date_period(
            'day',
            new DateTimeImmutable('2023-01-01'),
            new DateInterval('P1D'),
            new DateTimeImmutable('2023-01-11'),
            DatePeriod::EXCLUDE_START_DATE,
        );

        self::assertExtractedRowsEquals(
            rows(
                schema(date_schema('day')),
                row(['day' => new DateTimeImmutable('2023-01-02')]),
                row(['day' => new DateTimeImmutable('2023-01-03')]),
                row(['day' => new DateTimeImmutable('2023-01-04')]),
                row(['day' => new DateTimeImmutable('2023-01-05')]),
                row(['day' => new DateTimeImmutable('2023-01-06')]),
                row(['day' => new DateTimeImmutable('2023-01-07')]),
                row(['day' => new DateTimeImmutable('2023-01-08')]),
                row(['day' => new DateTimeImmutable('2023-01-09')]),
                row(['day' => new DateTimeImmutable('2023-01-10')]),
            ),
            $extractor,
        );
    }

    public function test_extracting_from_date_period_recurrences(): void
    {
        $extractor = from_sequence_date_period_recurrences(
            'day',
            new DateTimeImmutable('2023-01-01'),
            new DateInterval('P1D'),
            10,
            DatePeriod::EXCLUDE_START_DATE,
        );

        self::assertExtractedRowsEquals(
            rows(
                schema(date_schema('day')),
                row(['day' => new DateTimeImmutable('2023-01-02')]),
                row(['day' => new DateTimeImmutable('2023-01-03')]),
                row(['day' => new DateTimeImmutable('2023-01-04')]),
                row(['day' => new DateTimeImmutable('2023-01-05')]),
                row(['day' => new DateTimeImmutable('2023-01-06')]),
                row(['day' => new DateTimeImmutable('2023-01-07')]),
                row(['day' => new DateTimeImmutable('2023-01-08')]),
                row(['day' => new DateTimeImmutable('2023-01-09')]),
                row(['day' => new DateTimeImmutable('2023-01-10')]),
                row(['day' => new DateTimeImmutable('2023-01-11')]),
            ),
            $extractor,
        );
    }

    public function test_extracting_from_numbers_range(): void
    {
        $extractor = from_sequence_number('num', 0, 10, 1.5);

        self::assertExtractedRowsEquals(
            rows(
                schema(float_schema('num')),
                row(['num' => 0]),
                row(['num' => 1.5]),
                row(['num' => 3]),
                row(['num' => 4.5]),
                row(['num' => 6]),
                row(['num' => 7.5]),
                row(['num' => 9]),
            ),
            $extractor,
        );
    }

    public function test_a_heterogeneous_sequence_is_extracted_with_one_shape(): void
    {
        $extractor = new SequenceExtractor(new MixedSequenceGenerator(), 'code');

        static::assertEquals(
            [$extractor->schema(), $extractor->schema()],
            array_map(
                static fn(Rows $rows): Schema => $rows->schema(),
                iterator_to_array($extractor->extract(flow_context(config()))),
            ),
        );
    }
}
