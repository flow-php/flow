<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class ConcatTest extends FlowTestCase
{
    public function test_concat_arrays(): void
    {
        static::assertSame('["a"]["b","c"]', (new FunctionContext(flow_context()))->eval(
            concat(ref('array_1'), ref('array_2')),
            [
                'array_1' => ['a'],
                'array_2' => ['b', 'c'],
            ],
            schema(list_schema('array_1', type_list(type_string())), list_schema('array_2', type_list(type_string()))),
        ));
    }

    public function test_concat_different_types_of_values(): void
    {
        static::assertSame('1abc["a","b"]', (new FunctionContext(flow_context()))->eval(
            concat(lit(1), lit('a'), lit('b'), lit('c'), lit(['a', 'b'])),
            [],
            schema(),
        ));
    }

    public function test_concat_string_values(): void
    {
        static::assertSame('abc', (new FunctionContext(flow_context()))->eval(
            concat(lit('a'), lit('b'), lit('c')),
            [],
            schema(),
        ));
    }
}
