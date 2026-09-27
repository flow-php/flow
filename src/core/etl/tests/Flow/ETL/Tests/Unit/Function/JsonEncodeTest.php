<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;

use function Flow\ETL\DSL\array_to_row;
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
        $result = ref('value')
            ->jsonEncode()
            ->eval(array_to_row([
                'value' => new DateTimeImmutable('2021-01-01'),
            ], schema(datetime_schema('value'))), flow_context());

        static::assertInstanceOf(Json::class, $result);
        static::assertSame(
            '{"date":"2021-01-01 00:00:00.000000","timezone_type":3,"timezone":"UTC"}',
            $result->toString(),
        );
    }

    public function test_json_encode_on_integer(): void
    {
        static::assertSame('125', ref('value')
            ->jsonEncode()
            ->eval(array_to_row(['value' => 125], schema(int_schema('value'))), flow_context()));
    }

    public function test_json_encode_on_string(): void
    {
        static::assertSame('"test"', ref('value')
            ->jsonEncode()
            ->eval(array_to_row(['value' => 'test'], schema(str_schema('value'))), flow_context()));
    }

    public function test_json_encode_on_valid_associative_array(): void
    {
        $result = ref('value')
            ->jsonEncode()
            ->eval(array_to_row(['value' => [
                'value' => 1,
            ]], schema(map_schema('value', type_map(type_string(), type_integer())))), flow_context());

        static::assertInstanceOf(Json::class, $result);
        static::assertSame('{"value":1}', $result->toString());
    }
}
