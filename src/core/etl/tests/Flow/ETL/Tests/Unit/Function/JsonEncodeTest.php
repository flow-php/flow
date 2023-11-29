<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\array_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\str_entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class JsonEncodeTest extends TestCase
{
    public function test_json_encode_on_datetime() : void
    {
        $this->assertSame(
            '{"date":"2021-01-01 00:00:00.000000","timezone_type":3,"timezone":"UTC"}',
            ref('value')->jsonEncode()->eval(Row::create(datetime_entry('value', new \DateTimeImmutable('2021-01-01')))),
        );
    }

    public function test_json_encode_on_integer() : void
    {
        $this->assertSame(
            '125',
            ref('value')->jsonEncode()->eval(Row::create(int_entry('value', 125))),
        );
    }

    public function test_json_encode_on_string() : void
    {
        $this->assertSame(
            '"test"',
            ref('value')->jsonEncode()->eval(Row::create(str_entry('value', 'test'))),
        );
    }

    public function test_json_encode_on_valid_associative_array() : void
    {
        $this->assertSame(
            '{"value":1}',
            ref('value')->jsonEncode()->eval(Row::create(array_entry('value', ['value' => 1]))),
        );
    }
}
