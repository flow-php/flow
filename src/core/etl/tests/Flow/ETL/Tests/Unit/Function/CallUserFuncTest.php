<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

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

final class CallUserFuncTest extends FlowTestCase
{
    public function test_call_user_func_as_dsl(): void
    {
        // @mago-ignore analysis:possibly-invalid-argument
        static::assertIsInt(call('time')->eval(row(), flow_context()));
    }

    public function test_call_user_func_with_native_function(): void
    {
        $row = row(list_entry('list', [1, 2, 3], type_list(type_integer())));

        static::assertSame(3, ref('list')->call(lit('count'))->eval($row, flow_context()));
    }

    public function test_call_user_func_with_object_method(): void
    {
        $row = row(list_entry('list', [1, 2, 3], type_list(type_integer())));

        $calculator = new StaticCalculator();

        static::assertSame(3, ref('list')->call(lit($calculator->count(...)))->eval($row, flow_context()));
    }

    public function test_call_user_func_with_ref_alias_and_optional_arguments(): void
    {
        $row = row(string_entry('item_ids', '1,2,3'));

        static::assertSame(
            ['1', '2', '3'],
            ref('item_ids')->call(lit('explode'), ['separator' => ','], refAlias: 'string')->eval($row, flow_context()),
        );
    }

    public function test_call_user_func_with_ref_alias_and_optional_arguments_and_return_type(): void
    {
        $row = row(string_entry('item_ids', '1,2,3'));

        static::assertEquals(
            new ScalarResult([1, 2, 3], type_list(type_integer())),
            ref('item_ids')
                ->call(lit('explode'), ['separator' => ','], refAlias: 'string', returnType: type_list(type_integer()))
                ->eval($row, flow_context()),
        );
    }

    public function test_call_user_func_with_static_method(): void
    {
        $row = row(list_entry('list', [1, 2, 3], type_list(type_integer())));

        static::assertSame(3, ref('list')->call(lit(StaticCalculator::class . '::count'))->eval($row, flow_context()));
    }
}
