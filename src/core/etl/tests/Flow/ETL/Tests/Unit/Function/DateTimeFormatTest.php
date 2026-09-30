<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\date_time_format;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\now;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class DateTimeFormatTest extends FlowTestCase
{
    public function test_date_time_format(): void
    {
        static::assertEquals('2020-01-01 00:00:00', (new FunctionContext(flow_context()))->eval(
            date_time_format(ref('date_time'), 'Y-m-d H:i:s'),
            ['date_time' => new DateTimeImmutable('2020-01-01 00:00:00', new DateTimeZone('UTC'))],
            schema(datetime_schema('date_time')),
        ));
    }

    public function test_a_literal_format_over_a_zoned_column_equals_formatting_each_value(): void
    {
        $rows = [
            ['at' => new DateTimeImmutable('2026-03-29T00:59:59.123456Z')],
            ['at' => new DateTimeImmutable('2026-03-29T01:00:00Z')],
        ];

        foreach (['UTC', 'Europe/Warsaw'] as $zone) {
            static::assertSame(
                [
                    $rows[0]['at']->setTimezone(new DateTimeZone($zone))->format('Y-m-d\TH:i:s.uP T e'),
                    $rows[1]['at']->setTimezone(new DateTimeZone($zone))->format('Y-m-d\TH:i:s.uP T e'),
                ],
                (new FunctionContext(flow_context()))
                    ->column(
                        date_time_format(ref('at'), 'Y-m-d\TH:i:s.uP T e'),
                        $rows,
                        schema(datetime_schema('at', zone: $zone)),
                    )
                    ->values(),
            );
        }
    }

    public function test_a_literal_format_over_a_date_column(): void
    {
        static::assertSame(
            ['02/01/2026 00:00', '01/01/1970 00:00'],
            (new FunctionContext(flow_context()))
                ->column(
                    date_time_format(ref('on'), 'd/m/Y H:i'),
                    [['on' => new DateTimeImmutable('2026-01-02')], ['on' => new DateTimeImmutable('1970-01-01')]],
                    schema(date_schema('on')),
                )
                ->values(),
        );
    }

    public function test_a_null_in_a_datetime_column_is_refused_with_its_row_index(): void
    {
        try {
            (new FunctionContext(flow_context()))->column(
                date_time_format(ref('at'), 'Y-m-d'),
                [['at' => new DateTimeImmutable('2026-01-02')], ['at' => null], ['at' => null]],
                schema(datetime_schema('at', nullable: true)),
            );
            static::fail('a null datetime was formatted');
        } catch (EvaluationException $e) {
            static::assertSame(1, $e->rowIndex);
            static::assertInstanceOf(InvalidArgumentException::class, $e->getPrevious());
            static::assertSame('DateTimeFormat function requires non-null values', $e->getPrevious()->getMessage());
        }
    }

    public function test_formatting_now(): void
    {
        static::assertInstanceOf(DateTimeImmutable::class, (new FunctionContext(flow_context()))->eval(
            now(),
            ['date_time' => new DateTimeImmutable('2020-01-01 00:00:00', new DateTimeZone('UTC'))],
            schema(datetime_schema('date_time')),
        ));
    }

    public function test_invalid_date_time_format(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "object<DateTimeInterface>", got "string".');

        (new FunctionContext(flow_context()))->eval(
            date_time_format(ref('date_time'), 'Y-m-d H:i:s'),
            [
                'date_time' => '2020-01-01 00:00:00',
            ],
            schema(str_schema('date_time')),
        );
    }
}
