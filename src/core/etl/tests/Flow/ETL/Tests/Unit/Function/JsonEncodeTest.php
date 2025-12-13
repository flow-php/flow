<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{datetime_entry, flow_context, int_entry, json_entry, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;

final class JsonEncodeTest extends FlowTestCase
{
    public function test_json_encode_on_datetime() : void
    {
        $result = ref('value')->jsonEncode()->eval(row(datetime_entry('value', new \DateTimeImmutable('2021-01-01'))), flow_context());

        self::assertInstanceOf(Json::class, $result);
        self::assertSame('{"date":"2021-01-01 00:00:00.000000","timezone_type":3,"timezone":"UTC"}', $result->toString());
    }

    public function test_json_encode_on_integer() : void
    {
        self::assertSame(
            '125',
            ref('value')->jsonEncode()->eval(row(int_entry('value', 125)), flow_context()),
        );
    }

    public function test_json_encode_on_string() : void
    {
        self::assertSame(
            '"test"',
            ref('value')->jsonEncode()->eval(row(str_entry('value', 'test')), flow_context()),
        );
    }

    public function test_json_encode_on_valid_associative_array() : void
    {
        $result = ref('value')->jsonEncode()->eval(row(json_entry('value', ['value' => 1])), flow_context());

        self::assertInstanceOf(Json::class, $result);
        self::assertSame('{"value":1}', $result->toString());
    }
}
