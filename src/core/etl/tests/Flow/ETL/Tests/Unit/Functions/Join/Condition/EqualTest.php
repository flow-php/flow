<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions\Join\Condition;

use DateTimeImmutable;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Functions\Join\Condition\Equal;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class EqualTest extends FlowTestCase
{
    public function test_describe_round_trips(): void
    {
        $comparison = new Equal('a', ref('a'));
        $left = array_to_rows(
            [['a' => 1, 'b' => 'x'], ['a' => 2, 'b' => 'y'], ['a' => 3, 'b' => 'z']],
            schema(int_schema('a'), str_schema('b')),
        );
        $right = array_to_rows(
            [['a' => 1, 'b' => 'x'], ['a' => 9, 'b' => 'y'], ['a' => 3, 'b' => 'q']],
            schema(int_schema('a'), str_schema('b')),
        );

        static::assertSame(
            $comparison->compare($left, $right),
            DescribeRoundTrip::rebuilt($comparison)->compare($left, $right),
        );
    }

    public function test_failure(): void
    {
        static::assertSame(
            [false],
            (new Equal('datetime', 'datetime'))->compare(
                array_to_rows([[
                    'datetime' => new DateTimeImmutable('2022-10-01 00:00:00'),
                ]], schema(datetime_schema('datetime'))),
                array_to_rows([[
                    'datetime' => new DateTimeImmutable('2022-10-01 01:00:00'),
                ]], schema(datetime_schema('datetime'))),
            ),
        );
    }

    public function test_null_is_not_equal_to_null(): void
    {
        static::assertSame(
            [false],
            (new Equal('id', 'id'))->compare(
                array_to_rows([['id' => null]], schema(str_schema('id', nullable: true))),
                array_to_rows([['id' => null]], schema(str_schema('id', nullable: true))),
            ),
        );
    }

    public function test_null_is_not_equal_to_value(): void
    {
        static::assertSame(
            [false],
            (new Equal('id', 'id'))->compare(
                array_to_rows([['id' => null]], schema(int_schema('id', nullable: true))),
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
            ),
        );
        static::assertSame(
            [false],
            (new Equal('id', 'id'))->compare(
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
                array_to_rows([['id' => null]], schema(int_schema('id', nullable: true))),
            ),
        );
    }

    public function test_a_string_that_does_not_cast_to_the_other_side_throws(): void
    {
        $this->expectException(EvaluationException::class);
        $this->expectExceptionMessage(
            'Column "id" value \'f47ac10b-58cc-4372-a567-0e02b2c3d479\' cannot be cast to integer',
        );

        (new Equal('id', 'id'))->compare(
            array_to_rows([['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479']], schema(str_schema('id'))),
            array_to_rows([['id' => 1]], schema(int_schema('id'))),
        );
    }

    public function test_an_integral_string_past_two_to_the_53_casts_exactly(): void
    {
        static::assertSame(
            [true, false],
            (new Equal('id', 'id'))->compare(
                array_to_rows([['id' => '9007199254740993'], ['id' => '9007199254740993']], schema(str_schema('id'))),
                array_to_rows([
                    ['id' => 9_007_199_254_740_993],
                    ['id' => 9_007_199_254_740_992],
                ], schema(int_schema('id'))),
            ),
        );
    }

    public function test_a_numeric_string_casts_to_the_other_side(): void
    {
        static::assertSame(
            [true, false],
            (new Equal('id', 'id'))->compare(
                array_to_rows([['id' => '1'], ['id' => '1.5']], schema(str_schema('id'))),
                array_to_rows([['id' => 1], ['id' => 1]], schema(int_schema('id'))),
            ),
        );
    }

    public function test_uuid_objects_with_different_values_are_not_equal(): void
    {
        static::assertSame(
            [false],
            (new Equal('id', 'id'))->compare(
                array_to_rows([['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479']], schema(str_schema('id'))),
                array_to_rows([['id' => '00000000-0000-4000-8000-000000000000']], schema(str_schema('id'))),
            ),
        );
    }

    public function test_uuid_objects_with_same_value_are_equal(): void
    {
        static::assertSame(
            [true],
            (new Equal('id', 'id'))->compare(
                array_to_rows([['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479']], schema(str_schema('id'))),
                array_to_rows([['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479']], schema(str_schema('id'))),
            ),
        );
    }

    public function test_success(): void
    {
        static::assertSame(
            [true],
            (new Equal('datetime', 'datetime'))->compare(
                array_to_rows([[
                    'datetime' => $datetime = new DateTimeImmutable('2022-10-01 00:00:00'),
                ]], schema(datetime_schema('datetime'))),
                array_to_rows([['datetime' => $datetime]], schema(datetime_schema('datetime'))),
            ),
        );
    }

    public function test_compares_pair_aligned_batches(): void
    {
        static::assertSame(
            [true, false, false, true],
            (new Equal('a', 'b'))->compare(
                array_to_rows([
                    ['a' => 1],
                    ['a' => 2],
                    ['a' => null],
                    ['a' => 4],
                ], schema(int_schema('a', nullable: true))),
                array_to_rows([['b' => 1.0], ['b' => 3.0], ['b' => 1.0], ['b' => 4.0]], schema(float_schema('b'))),
            ),
        );
    }

    public function test_numeric_strings_are_equal_only_when_identical_as_under_equals(): void
    {
        static::assertSame(
            [false, true],
            (new Equal('k', 'k'))->compare(
                array_to_rows([['k' => '1'], ['k' => '1']], schema(str_schema('k'))),
                array_to_rows([['k' => '1.0'], ['k' => '1']], schema(str_schema('k'))),
            ),
        );
    }
}
