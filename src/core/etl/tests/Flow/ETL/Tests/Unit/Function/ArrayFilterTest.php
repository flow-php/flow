<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;

final class ArrayFilterTest extends FlowTestCase
{
    public function test_array_filter(): void
    {
        static::assertSame(
            [1],
            (new FunctionContext(flow_context()))->eval(
                ref('list')->arrayFilter(lit(2)),
                ['list' => [
                    1,
                    2,
                ]],
                schema(list_schema('list', type_list(type_integer()))),
            ),
        );
    }

    public function test_array_filter_by_entry_reference(): void
    {
        static::assertSame(
            [1],
            (new FunctionContext(flow_context()))->eval(
                ref('list')->arrayFilter(ref('int')),
                ['list' => [1, 2], 'int' => 2],
                schema(list_schema('list', type_list(type_integer())), int_schema('int')),
            ),
        );
    }

    public function test_array_filter_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "string".');

        $context = flow_context(config());
        (new FunctionContext($context))->eval(
            ref('map')->arrayFilter(lit(1)),
            ['map' => 'test'],
            schema(str_schema('map')),
        );
    }

    public function test_array_filter_not_existing_value(): void
    {
        static::assertSame(
            [1, 2],
            (new FunctionContext(flow_context()))->eval(
                ref('list')->arrayFilter(lit(5)),
                ['list' => [
                    1,
                    2,
                ]],
                schema(list_schema('list', type_list(type_integer()))),
            ),
        );
    }

    public function test_array_filter_on_non_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "string".');

        (new FunctionContext(flow_context()))->eval(
            ref('map')->arrayFilter(lit(1)),
            ['map' => 'test'],
            schema(str_schema('map')),
        );
    }
}
