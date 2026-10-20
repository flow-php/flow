<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateInterval;
use DateTimeImmutable;
use Flow\ETL\Function\ValueOrder;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

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

    public function test_nan_is_never_below_anything_in_either_direction(): void
    {
        $order = new ValueOrder();

        static::assertSame(1, $order->compare(NAN, 1.0));
        static::assertSame(1, $order->compare(1.0, NAN));
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
}
