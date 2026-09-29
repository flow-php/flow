<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ArrayMerge;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;

final class ArrayMergeTest extends FlowTestCase
{
    public function test_array_merge_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        $context = flow_context(config());
        (new FunctionContext($context))->eval(
            ref('a')->arrayMerge(ref('b')),
            ['a' => 1, 'b' => ['b' => 2]],
            schema(int_schema('a'), map_schema('b', type_map(type_string(), type_integer()))),
        );
    }

    public function test_array_merge_two_array_row_entries(): void
    {
        static::assertSame(
            ['a' => 1, 'b' => 2],
            (new FunctionContext(flow_context()))->eval(
                ref('a')->arrayMerge(ref('b')),
                ['a' => ['a' => 1], 'b' => ['b' => 2]],
                schema(
                    map_schema('a', type_map(type_string(), type_integer())),
                    map_schema('b', type_map(type_string(), type_integer())),
                ),
            ),
        );
    }

    public function test_array_merge_two_lit_functions(): void
    {
        $function = new ArrayMerge(lit(['a' => 1]), lit(['b' => 2]));

        static::assertSame(['a' => 1, 'b' => 2], (new FunctionContext(flow_context()))->eval($function, [], schema()));
    }

    public function test_array_merge_when_left_side_is_not_an_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        (new FunctionContext(flow_context()))->eval(
            ref('a')->arrayMerge(ref('b')),
            ['a' => 1, 'b' => ['b' => 2]],
            schema(int_schema('a'), map_schema('b', type_map(type_string(), type_integer()))),
        );
    }

    public function test_array_merge_when_right_side_is_not_an_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        (new FunctionContext(flow_context()))->eval(
            ref('a')->arrayMerge(ref('b')),
            ['a' => ['a' => 1], 'b' => 2],
            schema(map_schema('a', type_map(type_string(), type_integer())), int_schema('b')),
        );
    }
}
