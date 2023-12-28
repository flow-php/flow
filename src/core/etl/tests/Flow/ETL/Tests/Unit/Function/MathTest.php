<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use Flow\ETL\Function\Divide;
use Flow\ETL\Function\Minus;
use Flow\ETL\Function\Mod;
use Flow\ETL\Function\Multiply;
use Flow\ETL\Function\Plus;
use Flow\ETL\Function\Power;
use Flow\ETL\Function\Round;
use PHPUnit\Framework\TestCase;

final class MathTest extends TestCase
{
    public function test_divide() : void
    {
        $row = row(int_entry('a', 100), int_entry('b', 10));

        $this->assertSame(
            10,
            (new Divide(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_minus() : void
    {
        $row = row(int_entry('a', 100), int_entry('b', 100));

        $this->assertSame(
            0,
            (new Minus(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_modulo() : void
    {
        $row = row(int_entry('a', 110), int_entry('b', 100));

        $this->assertSame(
            10,
            (new Mod(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_multiple_operations() : void
    {
        $this->assertSame(
            200,
            ref('a')->plus(lit(100))->plus(lit(100))->minus(ref('b'))->eval(row(int_entry('a', 100), int_entry('b', 100)))
        );
    }

    public function test_multiply() : void
    {
        $row = row(int_entry('a', 100), int_entry('b', 100));

        $this->assertSame(
            10_000,
            (new Multiply(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_plus() : void
    {
        $row = row(int_entry('a', 100), int_entry('b', 100));

        $this->assertSame(
            200,
            (new Plus(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_power() : void
    {
        $row = row(int_entry('a', 1), int_entry('b', 2));

        $this->assertSame(
            1,
            (new Power(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_round() : void
    {
        $row = row(float_entry('a', 1.009), int_entry('b', 2));

        $this->assertSame(
            1.01,
            (new Round(ref('a'), ref('b')))->eval($row)
        );
    }
}
