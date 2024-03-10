<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{array_entry, datetime_entry, int_entry, ref, str_entry};
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class JsonEncodeTest extends TestCase
{
    public function test_json_encode_on_datetime() : void
    {
        self::assertSame(
            '{"date":"2021-01-01 00:00:00.000000","timezone_type":3,"timezone":"UTC"}',
            ref('value')->jsonEncode()->eval(Row::create(datetime_entry('value', new \DateTimeImmutable('2021-01-01')))),
        );
    }

    public function test_json_encode_on_integer() : void
    {
        self::assertSame(
            '125',
            ref('value')->jsonEncode()->eval(Row::create(int_entry('value', 125))),
        );
    }

    public function test_json_encode_on_string() : void
    {
        self::assertSame(
            '"test"',
            ref('value')->jsonEncode()->eval(Row::create(str_entry('value', 'test'))),
        );
    }

    public function test_json_encode_on_valid_associative_array() : void
    {
        self::assertSame(
            '{"value":1}',
            ref('value')->jsonEncode()->eval(Row::create(array_entry('value', ['value' => 1]))),
        );
    }
}
