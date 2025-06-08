<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{call, list_entry, lit, ref, row, string_entry};
use function Flow\Types\DSL\{type_integer, type_list};
use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Unit\Function\Fixtures\CallUserFunc\StaticCalculator;

final class CallUserFuncTest extends FlowTestCase
{
    public function test_call_user_func_as_dsl() : void
    {
        self::assertIsInt(call('time')->eval(row()));
    }

    public function test_call_user_func_with_native_function() : void
    {
        $row = row(
            list_entry('list', [1, 2, 3], type_list(type_integer())),
        );

        self::assertSame(
            3,
            ref('list')
                ->call(lit('count'))
                ->eval($row)
        );
    }

    public function test_call_user_func_with_object_method() : void
    {
        $row = row(
            list_entry('list', [1, 2, 3], type_list(type_integer())),
        );

        $calculator = new StaticCalculator();

        self::assertSame(
            3,
            ref('list')
                ->call(lit($calculator->count(...)))
                ->eval($row)
        );
    }

    public function test_call_user_func_with_ref_alias_and_optional_arguments() : void
    {
        $row = row(
            string_entry('item_ids', '1,2,3'),
        );

        self::assertSame(
            ['1', '2', '3'],
            ref('item_ids')
                ->call(lit('explode'), ['separator' => ','], refAlias: 'string')
                ->eval($row)
        );
    }

    public function test_call_user_func_with_ref_alias_and_optional_arguments_and_return_type() : void
    {
        $row = row(
            string_entry('item_ids', '1,2,3'),
        );

        self::assertEquals(
            new ScalarResult([1, 2, 3], type_list(type_integer())),
            ref('item_ids')
                ->call(lit('explode'), ['separator' => ','], refAlias: 'string', returnType: type_list(type_integer()))
                ->eval($row)
        );
    }

    public function test_call_user_func_with_static_method() : void
    {
        $row = row(
            list_entry('list', [1, 2, 3], type_list(type_integer())),
        );

        self::assertSame(
            3,
            ref('list')
                ->call(lit(StaticCalculator::class . '::count'))
                ->eval($row)
        );
    }
}
