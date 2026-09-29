<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;

final class JsonEncodeTest extends FlowTestCase
{
    public function test_json_encode_on_datetime(): void
    {
        // json_encode() of a non-container operand declares string, so the encoded object is a string
        static::assertSame('{"date":"2021-01-01 00:00:00.000000","timezone_type":3,"timezone":"UTC"}', (new FunctionContext(
            flow_context(),
        ))->eval(
            ref('value')->jsonEncode(),
            [
                'value' => new DateTimeImmutable('2021-01-01'),
            ],
            schema(datetime_schema('value')),
        ));
    }

    public function test_json_encode_on_integer(): void
    {
        static::assertSame('125', (new FunctionContext(flow_context()))->eval(
            ref('value')->jsonEncode(),
            ['value' => 125],
            schema(int_schema('value')),
        ));
    }

    public function test_json_encode_on_string(): void
    {
        static::assertSame('"test"', (new FunctionContext(flow_context()))->eval(
            ref('value')->jsonEncode(),
            ['value' => 'test'],
            schema(str_schema('value')),
        ));
    }

    public function test_json_encode_on_valid_associative_array(): void
    {
        // @mago-ignore analysis:mixed-assignment
        $result = (new FunctionContext(flow_context()))->eval(
            ref('value')->jsonEncode(),
            ['value' => [
                'value' => 1,
            ]],
            schema(map_schema('value', type_map(type_string(), type_integer()))),
        );

        static::assertInstanceOf(Json::class, $result);
        static::assertSame('{"value":1}', $result->toString());
    }
}
