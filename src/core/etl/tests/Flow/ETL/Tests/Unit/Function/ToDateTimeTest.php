<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\to_date_time;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ToDateTimeTest extends TestCase
{
    public function test_date_time_to_date_time() : void
    {
        $this->assertEquals(
            new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')),
            to_date_time(ref('date_time'))->eval(Row::create(datetime_entry('date_time', new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')))))
        );
    }

    public function test_int_to_date_time() : void
    {
        $this->assertEquals(
            new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')),
            to_date_time(ref('int'))->eval(Row::create(int_entry('int', (int) (new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')))->format('U'))))
        );
    }

    public function test_string_to_date_time() : void
    {
        $this->assertEquals(
            new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')),
            to_date_time(ref('string'), 'Y-m-d H:i:s')->eval(Row::create(str_entry('string', '2020-01-01 00:00:00')))
        );
    }
}
