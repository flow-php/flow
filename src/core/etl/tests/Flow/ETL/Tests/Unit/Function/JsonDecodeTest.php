<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class JsonDecodeTest extends FlowTestCase
{
    public function test_json_decode_expression(): void
    {
        static::assertSame(
            ['value' => 1],
            ref('value')->jsonDecode()->eval(row(str_entry('value', '{"value": 1}')), flow_context()),
        );
    }

    public function test_json_decode_expression_with_invalid_json(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('JsonDecode error: Syntax error');

        ref('value')->jsonDecode()->eval(row(str_entry('value', '{"value": 1')), flow_context());
    }

    public function test_json_decode_on_non_json_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('JsonDecode function requires string, array, or Json value');

        ref('value')->jsonDecode()->eval(row(int_entry('value', 125)), flow_context());
    }

    public function test_json_decode_on_scalar_json_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'JsonDecode function requires JSON that decodes to an array, cast scalar JSON instead',
        );

        ref('value')->jsonDecode()->eval(row(str_entry('value', '5')), flow_context());
    }
}
