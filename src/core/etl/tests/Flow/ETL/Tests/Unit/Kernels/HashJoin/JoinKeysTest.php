<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\HashJoin;

use Flow\ETL\Kernels\HashJoin\JoinKeys;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;

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
}
