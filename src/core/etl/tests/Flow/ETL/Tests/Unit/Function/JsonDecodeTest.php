<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{flow_context, int_entry, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class JsonDecodeTest extends FlowTestCase
{
    public function test_json_decode_expression() : void
    {
        self::assertSame(
            ['value' => 1],
            ref('value')->jsonDecode()->eval(row(str_entry('value', '{"value": 1}')), flow_context()),
        );
    }

    public function test_json_decode_expression_with_invalid_json() : void
    {
        self::assertNull(
            ref('value')->jsonDecode()->eval(row(str_entry('value', '{"value": 1')), flow_context()),
        );
    }

    public function test_json_decode_on_non_json_value() : void
    {
        self::assertNull(
            ref('value')->jsonDecode()->eval(row(int_entry('value', 125)), flow_context()),
        );
    }
}
