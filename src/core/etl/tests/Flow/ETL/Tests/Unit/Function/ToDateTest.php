<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_date;

final class ToDateTest extends FlowTestCase
{
    public function test_date_time_to_date(): void
    {
        static::assertEquals(
            new DateTimeImmutable('2020-01-01 00:00:00', new DateTimeZone('UTC')),
            to_date(ref('date_time'))
                ->eval(array_to_row(['date_time' => new DateTimeImmutable(
                    '2020-01-01 12:43:23',
                    new DateTimeZone('UTC'),
                )], schema(datetime_schema('date_time'))), flow_context()),
        );
    }

    public function test_int_to_date_time(): void
    {
        static::assertEquals(
            new DateTimeImmutable('2020-01-01 00:00:00', new DateTimeZone('UTC')),
            to_date(ref('int'))
                ->eval(array_to_row([
                    'int' => (int) (new DateTimeImmutable('2020-01-01 10:11:11', new DateTimeZone('UTC')))->format('U'),
                ], schema(int_schema('int'))), flow_context()),
        );
    }

    public function test_string_to_date_time(): void
    {
        static::assertEquals(
            new DateTimeImmutable('2020-01-01 00:00:00', new DateTimeZone('UTC')),
            to_date(ref('string'), 'Y-m-d H:i:s')->eval(array_to_row([
                'string' => '2020-01-01 10:08:00',
            ], schema(str_schema('string'))), flow_context()),
        );
    }
}
