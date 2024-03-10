<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{int_entry, ref, str_entry};
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class JsonDecodeTest extends TestCase
{
    public function test_json_decode_expression() : void
    {
        self::assertSame(
            ['value' => 1],
            ref('value')->jsonDecode()->eval(Row::create(str_entry('value', '{"value": 1}'))),
        );
    }

    public function test_json_decode_expression_with_invalid_json() : void
    {
        self::assertNull(
            ref('value')->jsonDecode()->eval(Row::create(str_entry('value', '{"value": 1'))),
        );
    }

    public function test_json_decode_on_non_json_value() : void
    {
        self::assertNull(
            ref('value')->jsonDecode()->eval(Row::create(int_entry('value', 125))),
        );
    }
}
