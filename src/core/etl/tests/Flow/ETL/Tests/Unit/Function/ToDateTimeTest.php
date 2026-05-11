<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\to_date_time;

final class ToDateTimeTest extends FlowTestCase
{
    public function test_date_time_to_date_time(): void
    {
        static::assertEquals(
            new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')),
            to_date_time(ref('date_time'))
                ->eval(
                    row(datetime_entry(
                        'date_time',
                        new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')),
                    )),
                    flow_context(),
                ),
        );
    }

    public function test_int_to_date_time(): void
    {
        static::assertEquals(
            new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')),
            to_date_time(ref('int'))
                ->eval(
                    row(int_entry(
                        'int',
                        (int) (new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')))->format('U'),
                    )),
                    flow_context(),
                ),
        );
    }

    public function test_string_to_date_time(): void
    {
        static::assertEquals(
            new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')),
            to_date_time(ref('string'), 'Y-m-d H:i:s')->eval(
                row(str_entry('string', '2020-01-01 00:00:00')),
                flow_context(),
            ),
        );
    }
}
