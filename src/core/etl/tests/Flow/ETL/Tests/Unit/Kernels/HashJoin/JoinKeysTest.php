<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\HashJoin;

use Flow\ETL\Kernels\HashJoin\JoinKeys;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;

final class JoinKeysTest extends FlowTestCase
{
    public function test_each_pair_keys_in_the_type_it_compares_in(): void
    {
        static::assertEquals(
            [type_integer(), type_float()],
            (new JoinKeys(['id', 'amount'], ['code', 'total']))->types(
                schema(int_schema('id'), int_schema('amount')),
                schema(str_schema('code'), float_schema('total')),
            ),
        );
    }

    public function test_a_side_without_a_schema_keys_in_its_own_types(): void
    {
        static::assertSame([], (new JoinKeys(['id'], ['code']))->types(schema(int_schema('id')), null));
    }

    public function test_both_encoders_give_equal_keys_to_values_equal_across_types(): void
    {
        [$left, $right] = (new JoinKeys(['id'], ['code']))->encoders(
            schema(int_schema('id')),
            schema(str_schema('code')),
        );

        static::assertSame(
            $left->of(array_to_rows([['id' => 7]], schema(int_schema('id')))),
            $right->of(array_to_rows([['code' => '7']], schema(str_schema('code')))),
        );
    }

    public function test_an_integral_string_key_past_two_to_the_53_meets_only_its_own_int(): void
    {
        [$left, $right] = (new JoinKeys(['id'], ['code']))->encoders(
            schema(int_schema('id')),
            schema(str_schema('code')),
        );
        $keys = $left->of(array_to_rows([
            ['id' => 9_007_199_254_740_993],
            ['id' => 9_007_199_254_740_992],
        ], schema(int_schema('id'))));
        $code = $right->of(array_to_rows([['code' => '9007199254740993']], schema(str_schema('code'))))[0];

        static::assertSame($keys[0], $code);
        static::assertNotSame($keys[1], $code);
    }

    public function test_left_keys_are_the_left_columns_cast_to_their_pair_type_named_by_position(): void
    {
        $keys = new JoinKeys(['id', 'code'], ['user_id', 'total']);

        $left = $keys->leftKeys(
            array_to_rows(
                [['id' => 1, 'code' => 'a', 'other' => true], ['id' => 2, 'code' => 'b', 'other' => false]],
                schema(int_schema('id'), str_schema('code'), bool_schema('other')),
            ),
            [0 => type_float()],
        );

        static::assertEquals(schema(float_schema('0'), str_schema('1')), $left->schema());
        static::assertSame([1.0, 2.0], $left->column('0')->values());
        static::assertSame(['a', 'b'], $left->column('1')->values());
    }

    public function test_right_keys_are_the_right_columns_named_by_position(): void
    {
        $right = (new JoinKeys(['id'], ['user_id']))->rightKeys(
            array_to_rows([['user_id' => 7, 'name' => 'x']], schema(int_schema('user_id'), str_schema('name'))),
            [],
        );

        static::assertEquals(schema(int_schema('0')), $right->schema());
        static::assertSame([7], $right->column('0')->values());
    }

    public function test_a_key_column_holding_a_null_is_nullable_and_one_without_is_not(): void
    {
        $nullable = schema(int_schema('id', nullable: true));
        $keys = new JoinKeys(['id'], ['id']);

        static::assertEquals(
            schema(int_schema('0', nullable: true)),
            $keys->leftKeys(array_to_rows([['id' => 1], ['id' => null]], $nullable), [])->schema(),
        );
        static::assertEquals(
            schema(int_schema('0')),
            $keys->leftKeys(array_to_rows([['id' => 1]], $nullable), [])->schema(),
        );
    }

    public function test_exact_keys_without_a_null_decide_the_condition(): void
    {
        static::assertTrue((new JoinKeys(['id'], ['id'], exact: true))->decides(array_to_rows([
            ['0' => 1],
            ['0' => 2],
        ], schema(int_schema('0')))));
    }

    public function test_keys_that_are_not_exact_never_decide_the_condition(): void
    {
        static::assertFalse((new JoinKeys(['id'], ['id']))->decides(array_to_rows([[
            '0' => 1,
        ]], schema(int_schema('0')))));
    }

    public function test_a_null_key_leaves_the_condition_to_the_pair_check(): void
    {
        static::assertFalse((new JoinKeys(['id'], ['id'], exact: true))->decides(array_to_rows([
            ['0' => 1],
            ['0' => null],
        ], schema(int_schema('0', nullable: true)))));
    }

    public function test_a_list_key_leaves_the_condition_to_the_pair_check(): void
    {
        static::assertFalse((new JoinKeys(['days'], ['days'], exact: true))->decides(array_to_rows([['0' => [
            1,
            2,
        ]]], schema(list_schema('0', type_list(type_integer()))))));
    }
}
