<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions\Join;

use Flow\ETL\Functions\Join\Condition\All;
use Flow\ETL\Functions\Join\Condition\Any;
use Flow\ETL\Functions\Join\Condition\Equal;
use Flow\ETL\Functions\Join\Condition\Identical;
use Flow\ETL\Functions\Join\Expression;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\col;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class ExpressionTest extends FlowTestCase
{
    public function test_describe_round_trips(): void
    {
        $expression = Expression::on(['a' => 'a', 'b' => 'b'], 'r_');
        $left = array_to_rows(
            [['a' => 1, 'b' => 'x'], ['a' => 2, 'b' => 'y'], ['a' => 3, 'b' => 'z']],
            schema(int_schema('a'), str_schema('b')),
        );
        $right = array_to_rows(
            [['a' => 1, 'b' => 'x'], ['a' => 9, 'b' => 'y'], ['a' => 3, 'b' => 'q']],
            schema(int_schema('a'), str_schema('b')),
        );

        $rebuilt = DescribeRoundTrip::rebuilt($expression);

        static::assertSame('r_', $rebuilt->prefix());
        static::assertSame(
            $expression->meet($left, [0, 1, 2], $right, [0, 1, 2]),
            $rebuilt->meet($left, [0, 1, 2], $right, [0, 1, 2]),
        );
    }

    public function test_expression(): void
    {
        $expression = Expression::on(new Equal('id', 'id'), '_');

        static::assertSame('_', $expression->prefix());
        static::assertEquals([col('id')], $expression->left());
        static::assertEquals([col('id')], $expression->right());
    }

    public function test_meet_compares_the_paired_rows(): void
    {
        static::assertSame(
            [true, false, true],
            Expression::on(new Equal('id', 'id'), '_')->meet(
                array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))),
                [0, 1, 1],
                array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))),
                [0, 0, 1],
            ),
        );
    }

    public function test_meet_of_no_pairs_is_empty(): void
    {
        static::assertSame(
            [],
            Expression::on(new Equal('id', 'id'))->meet(
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
                [],
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
                [],
            ),
        );
    }

    public function test_all_of_equal_comparisons_key_on_all_pairs(): void
    {
        $keys = Expression::on(new All(new Equal('id', 'user_id'), new Equal('country', 'country_code')))->keys();

        static::assertNotNull($keys);
        static::assertSame(['id', 'country'], $keys->left);
        static::assertSame(['user_id', 'country_code'], $keys->right);
    }

    public function test_an_any_comparison_has_no_keys(): void
    {
        static::assertNull(Expression::on(new Any(new Equal('id', 'id'), new Equal('email', 'email')))->keys());
    }

    public function test_all_with_any_keys_on_its_equalities(): void
    {
        $keys = Expression::on(
            new All(new Equal('id', 'user_id'), new Any(new Equal('a', 'a'), new Equal('b', 'b'))),
        )->keys();

        static::assertNotNull($keys);
        static::assertSame(['id'], $keys->left);
        static::assertSame(['user_id'], $keys->right);
    }

    public function test_all_of_only_non_equalities_has_no_keys(): void
    {
        static::assertNull(
            Expression::on(
                new All(
                    new Any(new Equal('a', 'a'), new Equal('b', 'b')),
                    new Any(new Equal('c', 'c'), new Equal('d', 'd')),
                ),
            )->keys(),
        );
    }

    public function test_nested_all_equalities_are_keys_next_to_an_any(): void
    {
        static::assertSame(
            ['id', 'country'],
            Expression::on(
                new All(
                    new All(new Equal('id', 'user_id'), new Equal('country', 'country_code')),
                    new Any(new Equal('a', 'a'), new Equal('b', 'b')),
                ),
            )->keys()?->left,
        );
    }

    public function test_a_duplicated_pair_is_one_key(): void
    {
        $keys = Expression::on(new All(new Equal('id', 'user_id'), new Equal('id', 'user_id')))->keys();

        static::assertNotNull($keys);
        static::assertSame(['id'], $keys->left);
        static::assertSame(['user_id'], $keys->right);
    }

    public function test_one_left_column_against_two_right_columns_is_two_keys(): void
    {
        $keys = Expression::on(new All(new Equal('id', 'user_id'), new Equal('id', 'account_id')))->keys();

        static::assertNotNull($keys);
        static::assertSame(['id', 'id'], $keys->left);
        static::assertSame(['user_id', 'account_id'], $keys->right);
    }

    public function test_an_identical_comparison_is_a_key(): void
    {
        $keys = Expression::on(new Identical('id', 'user_id'))->keys();

        static::assertNotNull($keys);
        static::assertSame(['id'], $keys->left);
        static::assertSame(['user_id'], $keys->right);
    }
}
