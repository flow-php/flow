<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use DateTimeImmutable;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class TemporalStringComparisonTest extends FlowIntegrationTestCase
{
    public function test_string_bounds_select_rows_by_a_datetime_column(): void
    {
        static::assertSame(
            ['b', 'c'],
            df()
                ->read(from_array(
                    [
                        ['id' => 'a', 'ts' => new DateTimeImmutable('2026-10-01 00:00:00 UTC')],
                        ['id' => 'b', 'ts' => new DateTimeImmutable('2026-10-02 09:00:00 UTC')],
                        ['id' => 'c', 'ts' => new DateTimeImmutable('2026-10-03 00:00:00 UTC')],
                    ],
                    schema(str_schema('id'), datetime_schema('ts')),
                ))
                ->filter(ref('ts')->between(lit('2026-10-02'), lit('2026-10-03'), Boundary::INCLUSIVE))
                ->fetch()
                ->reduceToArray('id'),
        );
    }

    public function test_a_string_column_is_compared_as_a_datetime(): void
    {
        static::assertSame(
            ['b', 'c'],
            df()
                ->read(from_array(
                    [
                        ['id' => 'a', 'ts' => new DateTimeImmutable('2026-10-01 00:00:00 UTC'), 's' => null],
                        [
                            'id' => 'b',
                            'ts' => new DateTimeImmutable('2026-10-02 09:00:00 UTC'),
                            's' => '2026-10-02 09:00:00',
                        ],
                        ['id' => 'c', 'ts' => new DateTimeImmutable('2026-10-03 00:00:00 UTC'), 's' => '2026-10-03'],
                    ],
                    schema(str_schema('id'), datetime_schema('ts'), str_schema('s', nullable: true)),
                ))
                ->filter(ref('ts')->equals(ref('s')))
                ->fetch()
                ->reduceToArray('id'),
        );
    }

    public function test_a_string_that_is_not_a_date_fails_the_evaluation(): void
    {
        $this->expectException(EvaluationException::class);
        $this->expectExceptionMessage('Cast function failed: Can\'t cast "string" into "datetime" type (row 0)');

        df()
            ->read(from_array(
                [['id' => 'a', 'ts' => new DateTimeImmutable('2026-10-01 00:00:00 UTC')]],
                schema(str_schema('id'), datetime_schema('ts')),
            ))
            ->filter(ref('ts')->greaterThan(lit('not a date')))
            ->fetch();
    }
}
