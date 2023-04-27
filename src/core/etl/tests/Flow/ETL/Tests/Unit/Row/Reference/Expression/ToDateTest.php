<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_date;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ToDateTest extends TestCase
{
    public function test_date_time_to_date() : void
    {
        $this->assertEquals(
            new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')),
            to_date(ref('date_time'))->eval(Row::create(Entry::datetime('date_time', new \DateTimeImmutable('2020-01-01 12:43:23', new \DateTimeZone('UTC')))))
        );
    }

    public function test_int_to_date_time() : void
    {
        $this->assertEquals(
            new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')),
            to_date(ref('int'))->eval(Row::create(Entry::int('int', (int) (new \DateTimeImmutable('2020-01-01 10:11:11', new \DateTimeZone('UTC')))->format('U'))))
        );
    }

    public function test_string_to_date_time() : void
    {
        $this->assertEquals(
            new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')),
            to_date(ref('string'), 'Y-m-d H:i:s')->eval(Row::create(Entry::str('string', '2020-01-01 10:08:00')))
        );
    }
}
