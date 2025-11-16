<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{call, flow_context, list_entry, lit, ref, row, string_entry};
use function Flow\Types\DSL\{type_integer, type_list};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\Tests\Unit\Function\Fixtures\CallUserFunc\StaticCalculator;
use PHPUnit\Framework\TestCase;

final class CallUserFuncTest extends TestCase
{
    public function test_call_user_func_as_dsl() : void
    {
        self::assertIsInt(call('time')->eval(row(), flow_context()));
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
                ->eval($row, flow_context())
        );
    }

    public function test_call_user_func_with_native_function_and_no_arguments() : void
    {
        $row = row(
            list_entry('list', [1, 2, 3], type_list(type_integer())),
        );

        self::assertNull(
            ref('list')
                ->call(lit('time'))
                ->eval($row, flow_context())
        );
    }

    public function test_call_user_func_with_non_callable_function() : void
    {
        $row = row(
            list_entry('list', [1, 2, 3], type_list(type_integer())),
        );

        self::assertNull(
            ref('list')
                ->call(lit('unknown'), refAlias: 'whatever')
                ->eval($row, flow_context())
        );
    }

    public function test_call_user_func_with_non_string_argument_keys() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('call arguments cannot be a list');

        ref('list')
            ->call(lit('explode'), arguments: [',']); // @phpstan-ignore argument.type
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
                ->eval($row, flow_context())
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
                ->call(lit('explode'), arguments: ['separator' => ','], refAlias: 'string')
                ->eval($row, flow_context())
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
                ->call(lit('explode'), refAlias: 'string', arguments: ['separator' => ','], returnType: type_list(type_integer()))
                ->eval($row, flow_context())
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
                ->eval($row, flow_context())
        );
    }

    public function test_call_user_func_with_without_ref_alias_and_arguments() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('refAlias cannot be null when named arguments are passed');

        ref('item_ids')
            ->call(lit('explode'), arguments: ['separator' => ',']);
    }
}
