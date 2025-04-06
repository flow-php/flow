<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{float_entry, int_entry, integer_entry, lit, ref, row, type_integer};
use Flow\ETL\Function\{Divide, Minus, Mod, Multiply, Plus, Power, Round};
use Flow\ETL\Tests\FlowTestCase;

final class MathTest extends FlowTestCase
{
    public function test_divide() : void
    {
        $row = row(int_entry('a', 100), int_entry('b', 10));

        self::assertSame(
            10,
            (new Divide(ref('a'), ref('b')))->eval($row)?->value
        );
    }

    public function test_divide_scale() : void
    {
        self::assertSame(1, ref('a')->divide(0.1)->eval(row(float_entry('a', 2.0, precision: 1)))?->type->precision);
        self::assertSame(6, ref('a')->divide(0.1, scale: 6)->eval(row(float_entry('a', 2.0, precision: 1)))?->type->precision);
        self::assertSame(6, ref('a')->divide(0.1, scale: 6)->eval(row(float_entry('a', 2.0000008, precision: 7)))?->type->precision);
        self::assertSame(6, ref('a')->divide(1, scale: 6)->eval(row(float_entry('a', 2.0000008, precision: 7)))?->type->precision);
        self::assertSame(6, ref('a')->divide(1, scale: 6)->eval(row(integer_entry('a', 10)))?->type->precision);
        self::assertEquals(type_integer(), ref('a')->divide(1)->eval(row(integer_entry('a', 10)))?->type);
        self::assertEquals(type_integer(), ref('a')->divide(1)->eval(row(integer_entry('a', 10)))?->type);
    }

    public function test_minus() : void
    {
        $row = row(int_entry('a', 100), int_entry('b', 100));

        self::assertSame(
            0,
            (new Minus(ref('a'), ref('b')))->eval($row)?->value
        );
    }

    public function test_minus_scale() : void
    {
        self::assertSame(1, ref('a')->minus(0.1)->eval(row(float_entry('a', 2.0, precision: 1)))?->type->precision);
        self::assertSame(6, ref('a')->minus(0.1, scale: 6)->eval(row(float_entry('a', 2.0, precision: 1)))?->type->precision);
        self::assertSame(6, ref('a')->minus(0.1, scale: 6)->eval(row(float_entry('a', 2.0000008, precision: 7)))?->type->precision);
        self::assertSame(6, ref('a')->minus(1, scale: 6)->eval(row(float_entry('a', 2.0000008, precision: 7)))?->type->precision);
        self::assertEquals(type_integer(), ref('a')->minus(1, scale: 6)->eval(row(integer_entry('a', 10)))?->type);
        self::assertEquals(type_integer(), ref('a')->minus(1)->eval(row(integer_entry('a', 10)))?->type);
        self::assertEquals(type_integer(), ref('a')->minus(1)->eval(row(integer_entry('a', 10)))?->type);
    }

    public function test_mod_scale() : void
    {
        self::assertSame(1, ref('a')->mod(0.1)->eval(row(float_entry('a', 2.0, precision: 1)))?->type->precision);
        self::assertSame(6, ref('a')->mod(0.1, scale: 6)->eval(row(float_entry('a', 2.0, precision: 1)))?->type->precision);
        self::assertSame(6, ref('a')->mod(0.1, scale: 6)->eval(row(float_entry('a', 2.0000008, precision: 7)))?->type->precision);
        self::assertSame(6, ref('a')->mod(1, scale: 6)->eval(row(float_entry('a', 2.0000008, precision: 7)))?->type->precision);
        self::assertEquals(type_integer(), ref('a')->mod(1, scale: 6)->eval(row(integer_entry('a', 10)))?->type);
        self::assertEquals(type_integer(), ref('a')->mod(1)->eval(row(integer_entry('a', 10)))?->type);
        self::assertEquals(type_integer(), ref('a')->mod(1)->eval(row(integer_entry('a', 10)))?->type);
    }

    public function test_modulo() : void
    {
        $row = row(int_entry('a', 110), int_entry('b', 100));

        self::assertSame(
            10,
            (new Mod(ref('a'), ref('b')))->eval($row)?->value
        );
    }

    public function test_multiple_operations() : void
    {
        self::assertSame(
            200,
            ref('a')->plus(lit(100))->plus(lit(100))->minus(ref('b'))->eval(row(int_entry('a', 100), int_entry('b', 100)))?->value
        );
    }

    public function test_multiply() : void
    {
        $row = row(int_entry('a', 100), int_entry('b', 100));

        self::assertSame(
            10_000,
            (new Multiply(ref('a'), ref('b')))->eval($row)?->value
        );
    }

    public function test_multiply_resolving_scale() : void
    {
        self::assertSame(1, ref('a')->multiply(0.1)->eval(row(float_entry('a', 2.0, precision: 1)))?->type->precision);
        self::assertSame(6, ref('a')->multiply(0.1, scale: 6)->eval(row(float_entry('a', 2.0, precision: 1)))?->type->precision);
        self::assertSame(6, ref('a')->multiply(0.1, scale: 6)->eval(row(float_entry('a', 2.0000008, precision: 7)))?->type->precision);
        self::assertSame(6, ref('a')->multiply(1, scale: 6)->eval(row(float_entry('a', 2.0000008, precision: 7)))?->type->precision);
        self::assertEquals(type_integer(), ref('a')->multiply(1, scale: 6)->eval(row(integer_entry('a', 10)))?->type);
        self::assertEquals(type_integer(), ref('a')->multiply(1)->eval(row(integer_entry('a', 10)))?->type);
        self::assertEquals(type_integer(), ref('a')->multiply(1)->eval(row(integer_entry('a', 10)))?->type);
    }

    public function test_plus() : void
    {
        $row = row(int_entry('a', 100), int_entry('b', 100));

        self::assertSame(
            200,
            (new Plus(ref('a'), ref('b')))->eval($row)?->value
        );
    }

    public function test_plus_scale() : void
    {
        self::assertSame(1, ref('a')->plus(0.1)->eval(row(float_entry('a', 2.0, precision: 1)))?->type->precision);
        self::assertSame(6, ref('a')->plus(0.1, scale: 6)->eval(row(float_entry('a', 2.0, precision: 1)))?->type->precision);
        self::assertSame(6, ref('a')->plus(0.1, scale: 6)->eval(row(float_entry('a', 2.0000008, precision: 7)))?->type->precision);
        self::assertSame(6, ref('a')->plus(1, scale: 6)->eval(row(float_entry('a', 2.0000008, precision: 7)))?->type->precision);
        self::assertEquals(type_integer(), ref('a')->plus(1, scale: 6)->eval(row(integer_entry('a', 10)))?->type);
        self::assertEquals(type_integer(), ref('a')->plus(1)->eval(row(integer_entry('a', 10)))?->type);
        self::assertEquals(type_integer(), ref('a')->plus(1)->eval(row(integer_entry('a', 10)))?->type);
    }

    public function test_pow_scale() : void
    {
        self::assertSame(1, ref('a')->power(1)->eval(row(float_entry('a', 2.0, precision: 1)))?->type->precision);
        self::assertSame(6, ref('a')->power(1, scale: 6)->eval(row(float_entry('a', 2.0, precision: 1)))?->type->precision);
        self::assertSame(6, ref('a')->power(1, scale: 6)->eval(row(float_entry('a', 2.0000008, precision: 7)))?->type->precision);
        self::assertSame(6, ref('a')->power(1, scale: 6)->eval(row(float_entry('a', 2.0000008, precision: 7)))->type->precision);
        self::assertEquals(type_integer(), ref('a')->minus(1, scale: 6)->eval(row(integer_entry('a', 10)))?->type);
        self::assertEquals(type_integer(), ref('a')->minus(1)->eval(row(integer_entry('a', 10)))?->type);
        self::assertEquals(type_integer(), ref('a')->minus(1)->eval(row(integer_entry('a', 10)))?->type);
    }

    public function test_power() : void
    {
        $row = row(int_entry('a', 1), int_entry('b', 2));

        self::assertSame(
            1,
            (new Power(ref('a'), ref('b')))->eval($row)?->value
        );
    }

    public function test_round() : void
    {
        $row = row(float_entry('a', 1.009), int_entry('b', 2));

        self::assertSame(
            1.01,
            (new Round(ref('a'), ref('b')))->eval($row)
        );
    }
}
