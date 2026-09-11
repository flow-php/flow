<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\CallUserFunc;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Unit\Function\Fixtures\CallUserFunc\StaticCalculator;
use Flow\ETL\Transformer\ScalarFunctionTransformer;

use function Flow\ETL\DSL\call;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\row_number;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class CallUserFuncTest extends FlowTestCase
{
    public function test_named_parameters_survive_a_rebuild(): void
    {
        $function = new CallUserFunc(lit('explode'), type_list(type_string()), [
            'separator' => lit(','),
            'string' => lit('1,2,3'),
        ]);
        $rebuilt = $function->withChildren($function->children());

        static::assertEquals($function->eval(row([]), flow_context()), $rebuilt->eval(row([]), flow_context()));
        static::assertEquals(['1', '2', '3'], $rebuilt->eval(row([]), flow_context()));
    }

    public function test_call_user_func_as_dsl(): void
    {
        static::assertIsInt(call(lit('time'), type_integer())->eval(row([]), flow_context()));
    }

    public function test_a_non_scalar_function_cannot_replace_the_callable_child(): void
    {
        $function = new CallUserFunc(lit('count'), type_integer(), [lit([1, 2, 3])]);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('CallUserFunc requires a ScalarFunction as its first child');

        $function->withChildren([row_number(), lit([1, 2, 3])]);
    }

    public function test_call_user_func_with_array_callable(): void
    {
        static::assertSame(3, ref('list')
            ->call(lit([new StaticCalculator(), 'count']), type_integer())
            ->eval(row(['list' => [1, 2, 3]]), flow_context()));
    }

    public function test_call_user_func_with_native_function(): void
    {
        $row = row(['list' => [1, 2, 3]]);

        static::assertSame(3, ref('list')->call(lit('count'), type_integer())->eval($row, flow_context()));
    }

    public function test_call_user_func_with_ref_alias_and_optional_arguments(): void
    {
        $row = row(['item_ids' => '1,2,3']);

        static::assertEquals(
            ['1', '2', '3'],
            ref('item_ids')
                ->call(lit('explode'), type_list(type_string()), ['separator' => ','], refAlias: 'string')
                ->eval($row, flow_context()),
        );
    }

    public function test_call_user_func_with_ref_alias_and_optional_arguments_and_return_type(): void
    {
        $row = row(['item_ids' => '1,2,3']);

        static::assertEquals(
            [1, 2, 3],
            ref('item_ids')
                ->call(lit('explode'), type_list(type_integer()), ['separator' => ','], refAlias: 'string')
                ->eval($row, flow_context()),
        );
    }

    public function test_call_user_func_with_static_method(): void
    {
        $row = row(['list' => [1, 2, 3]]);

        static::assertSame(3, ref('list')
            ->call(lit(StaticCalculator::class . '::count'), type_integer())
            ->eval($row, flow_context()));
    }

    public function test_a_callable_that_can_return_null_declares_a_nullable_column(): void
    {
        static::assertSame('?string', (new CallUserFunc(lit('strtoupper'), type_string(), [ref(
            'name',
        )]))->returns()->toString());
    }

    public function test_a_null_return_lands_in_a_nullable_column(): void
    {
        $result = (new ScalarFunctionTransformer(
            'out',
            new CallUserFunc(lit([StaticCalculator::class, 'alwaysNull']), type_string(), []),
        ))->transform(rows(schema(str_schema('name')), row(['name' => 'a'])), flow_context());

        static::assertTrue($result->schema()->get('out')->isNullable());
        static::assertNull($result->first()->get('out'));
    }
}
