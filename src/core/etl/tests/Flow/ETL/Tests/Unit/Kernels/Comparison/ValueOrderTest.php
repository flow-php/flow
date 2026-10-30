<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\Comparison;

use DateInterval;
use DateTimeImmutable;
use Flow\ETL\Kernels\Comparison\ValueOrder;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_numeric_string;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;

use const NAN;

final class ValueOrderTest extends FlowTestCase
{
    #[TestWith(['9', '10', 1])]
    #[TestWith(['10', '9', -1])]
    #[TestWith(['a', 'a', 0])]
    #[TestWith(['B', 'a', -1])]
    public function test_two_strings_order_by_bytes(string $left, string $right, int $expected): void
    {
        static::assertSame($expected, (new ValueOrder(type_string(), type_string()))->compare($left, $right));
    }

    #[TestWith(['9', '10', -1])]
    #[TestWith(['18446744073709551615', '18446744073709551614', 1])]
    #[TestWith(['100', '100', 0])]
    public function test_a_numeric_string_operand_orders_strings_numerically(
        string $left,
        string $right,
        int $expected,
    ): void {
        static::assertSame($expected, (new ValueOrder(type_optional(type_numeric_string()), type_string()))->compare(
            $left,
            $right,
        ));
    }

    public function test_an_order_without_operand_types_orders_strings_by_bytes(): void
    {
        static::assertSame(1, (new ValueOrder())->compare('9', '10'));
    }

    public function test_numbers_order_as_php_compares_them(): void
    {
        $order = new ValueOrder();

        static::assertSame(-1, $order->compare(1, 1.5));
        static::assertSame(0, $order->compare(2, 2.0));
        static::assertSame(1, $order->compare('5', 3));
    }

    public function test_nan_is_the_largest_value_and_equals_itself(): void
    {
        $order = new ValueOrder();

        static::assertSame(1, $order->compare(NAN, 1.0));
        static::assertSame(-1, $order->compare(1.0, NAN));
        static::assertSame(0, $order->compare(NAN, NAN));
    }

    public function test_float_operands_rank_nan_above_every_number(): void
    {
        $order = new ValueOrder(type_float(), type_float());

        static::assertSame([false, true, false, null], $order->lessThan([NAN, 1.0, NAN, null], [1.0, NAN, NAN, 1.0]));
        static::assertSame(
            [false, true, true, null],
            $order->lessThanOrEqual([NAN, 1.0, NAN, null], [1.0, NAN, NAN, 1.0]),
        );
    }

    public function test_date_times_order_by_instant(): void
    {
        static::assertSame(-1, (new ValueOrder())->compare(
            new DateTimeImmutable('2024-01-01 10:00:00+00:00'),
            new DateTimeImmutable('2024-01-01 11:30:00+01:00'),
        ));
    }

    public function test_intervals_order_by_length(): void
    {
        $order = new ValueOrder();

        static::assertSame(-1, $order->compare(new DateInterval('PT59M'), new DateInterval('PT1H')));
        static::assertSame(0, $order->compare(new DateInterval('PT60M'), new DateInterval('PT1H')));
    }

    public function test_less_than_over_scalar_columns_is_php_order_with_nulls_kept(): void
    {
        $order = new ValueOrder(type_integer(), type_float());

        static::assertSame(
            [true, false, false, null, null],
            $order->lessThan([1, 2, 3, null, 4], [1.5, 2.0, 1.0, 1.0, null]),
        );
        static::assertSame([true, true, false, null], $order->lessThanOrEqual([1, 2, 3, null], [1.5, 2.0, 1.0, 1.0]));
    }

    public function test_less_than_over_string_columns_orders_by_bytes(): void
    {
        $order = new ValueOrder(type_string(), type_string());

        static::assertSame([false, true, null], $order->lessThan(['9', '10', null], ['10', '9', 'a']));
        static::assertSame([false, true, true], $order->lessThanOrEqual(['9', '10', 'a'], ['10', '9', 'a']));
    }

    public function test_less_than_with_a_numeric_string_operand_orders_numerically(): void
    {
        static::assertSame([true], (new ValueOrder(type_numeric_string(), type_string()))->lessThan(['9'], ['10']));
    }

    public function test_less_than_over_intervals_orders_by_length(): void
    {
        static::assertSame(
            [true, false],
            (new ValueOrder())->lessThanOrEqual([new DateInterval('PT60M'), new DateInterval('PT61M')], [
                new DateInterval('PT1H'),
                new DateInterval('PT1H'),
            ]),
        );
    }

    public function test_a_boolean_and_an_integer_operand_compare_as_scalars(): void
    {
        static::assertSame(
            [true, false],
            (new ValueOrder(type_boolean(), type_integer()))->lessThan([false, true], [1, 1]),
        );
    }
}
