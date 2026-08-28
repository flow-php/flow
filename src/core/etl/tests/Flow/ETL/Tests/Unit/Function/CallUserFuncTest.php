<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\CallUserFunc;
use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Unit\Function\Fixtures\CallUserFunc\StaticCalculator;

use function Flow\ETL\DSL\call;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\string_entry;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class CallUserFuncTest extends FlowTestCase
{
    public function test_named_parameters_survive_a_rebuild(): void
    {
        $function = new CallUserFunc(
            lit('explode'),
            ['separator' => lit(','), 'string' => lit('1,2,3')],
            type_list(type_string()),
        );
        $rebuilt = $function->withChildren($function->children());

        static::assertEquals($function->eval(row(), flow_context()), $rebuilt->eval(row(), flow_context()));
        static::assertEquals(['1', '2', '3'], $rebuilt->eval(row(), flow_context())->value);
    }

    public function test_call_user_func_as_dsl(): void
    {
        // @mago-ignore analysis:possibly-invalid-argument
        static::assertIsInt(call('time', type_integer())->eval(row(), flow_context())->value);
    }

    public function test_call_user_func_with_native_function(): void
    {
        $row = row(list_entry('list', [1, 2, 3], type_list(type_integer())));

        static::assertSame(3, ref('list')->call(lit('count'), type_integer())->eval($row, flow_context())->value);
    }

    public function test_call_user_func_with_object_method(): void
    {
        $row = row(list_entry('list', [1, 2, 3], type_list(type_integer())));

        $calculator = new StaticCalculator();

        static::assertSame(
            3,
            ref('list')->call(lit($calculator->count(...)), type_integer())->eval($row, flow_context())->value,
        );
    }

    public function test_call_user_func_with_ref_alias_and_optional_arguments(): void
    {
        $row = row(string_entry('item_ids', '1,2,3'));

        static::assertEquals(
            new ScalarResult(['1', '2', '3'], type_list(type_string())),
            ref('item_ids')
                ->call(lit('explode'), type_list(type_string()), ['separator' => ','], refAlias: 'string')
                ->eval($row, flow_context()),
        );
    }

    public function test_call_user_func_with_ref_alias_and_optional_arguments_and_return_type(): void
    {
        $row = row(string_entry('item_ids', '1,2,3'));

        static::assertEquals(
            new ScalarResult([1, 2, 3], type_list(type_integer())),
            ref('item_ids')
                ->call(lit('explode'), type_list(type_integer()), ['separator' => ','], refAlias: 'string')
                ->eval($row, flow_context()),
        );
    }

    public function test_call_user_func_with_static_method(): void
    {
        $row = row(list_entry('list', [1, 2, 3], type_list(type_integer())));

        static::assertSame(
            3,
            ref('list')
                ->call(lit(StaticCalculator::class . '::count'), type_integer())
                ->eval($row, flow_context())
                ->value,
        );
    }
}
