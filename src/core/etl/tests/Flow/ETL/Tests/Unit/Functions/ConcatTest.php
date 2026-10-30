<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Functions\Concat;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Context\ScalarFunctionFixtures;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
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

    public function test_concat_skips_nulls(): void
    {
        static::assertSame('ac', (new FunctionContext(flow_context()))->eval(
            concat(ref('a'), ref('b'), lit('c')),
            ['a' => 'a', 'b' => null],
            schema(str_schema('a'), str_schema('b', true)),
        ));
    }

    public function test_concat_without_arguments_is_an_empty_string(): void
    {
        static::assertSame('', (new FunctionContext(flow_context()))->eval(concat(), [], schema()));
    }

    public function test_describe_round_trips(): void
    {
        $context = new FunctionContext();
        $function = new Concat(ref('string'), lit('-'), ref('integer'));
        $rebuilt = DescribeRoundTrip::rebuilt($function);
        $rows = ScalarFunctionFixtures::rows(new PhpBackend());
        $schema = ScalarFunctionFixtures::schema();

        static::assertSame($context->exported($function, $rows, $schema), $context->exported($rebuilt, $rows, $schema));
    }
}
