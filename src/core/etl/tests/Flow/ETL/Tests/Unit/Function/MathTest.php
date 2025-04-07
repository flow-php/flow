<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{float_entry, int_entry, lit, ref, row};
use Flow\ETL\Function\{Divide, Minus, Mod, Multiply, Plus, Power, Round};
use Flow\ETL\Tests\FlowTestCase;

final class MathTest extends FlowTestCase
{
    public function test_divide() : void
    {
        $row = row(int_entry('a', 100), int_entry('b', 10));

        self::assertSame(
            10,
            (new Divide(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_minus() : void
    {
        $row = row(int_entry('a', 100), int_entry('b', 100));

        self::assertSame(
            0,
            (new Minus(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_modulo() : void
    {
        $row = row(int_entry('a', 110), int_entry('b', 100));

        self::assertSame(
            10,
            (new Mod(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_multiple_operations() : void
    {
        self::assertSame(
            200,
            ref('a')->plus(lit(100))->plus(lit(100))->minus(ref('b'))->eval(row(int_entry('a', 100), int_entry('b', 100)))
        );
    }

    public function test_multiply() : void
    {
        $row = row(int_entry('a', 100), int_entry('b', 100));

        self::assertSame(
            10_000,
            (new Multiply(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_plus() : void
    {
        $row = row(int_entry('a', 100), int_entry('b', 100));

        self::assertSame(
            200,
            (new Plus(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_power() : void
    {
        $row = row(int_entry('a', 1), int_entry('b', 2));

        self::assertSame(
            1,
            (new Power(ref('a'), ref('b')))->eval($row)
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
