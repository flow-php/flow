<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{call_method, datetime_entry, ref, str_entry};
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class CallMethodTest extends TestCase
{
    public function test_call_method() : void
    {
        $row = Row::create(
            datetime_entry('object', '2023-01-01 00:00:00 UTC'),
            str_entry('method', 'format'),
            str_entry('method_param', 'H:i:s Y-m-d'),
        );

        self::assertEquals(
            '00:00:00 2023-01-01',
            call_method(
                ref('object'),
                ref('method'),
                [ref('method_param')],
            )->eval($row)
        );
    }

    public function test_method_not_string() : void
    {
        $row = Row::create(
            datetime_entry('object', '2023-01-01 00:00:00 UTC'),
            datetime_entry('method', '2023-01-01 00:00:00 UTC'),
        );

        self::assertNull(
            call_method(
                ref('object'),
                ref('method')
            )->eval($row)
        );
    }

    public function test_not_existing_method() : void
    {
        $row = Row::create(
            datetime_entry('object', '2023-01-01 00:00:00 UTC'),
            str_entry('method', 'method_that_not_exists'),
        );

        self::assertNull(
            call_method(
                ref('object'),
                ref('method')
            )->eval($row)
        );
    }

    public function test_null_method() : void
    {
        $row = Row::create(
            datetime_entry('object', '2023-01-01 00:00:00 UTC'),
            str_entry('method', null),
        );

        self::assertNull(
            call_method(
                ref('object'),
                ref('method')
            )->eval($row)
        );
    }

    public function test_null_object() : void
    {
        $row = Row::create(
            str_entry('object', null),
            str_entry('method', 'getTimestamp'),
        );

        self::assertNull(
            call_method(
                ref('object'),
                ref('method')
            )->eval($row)
        );
    }
}
