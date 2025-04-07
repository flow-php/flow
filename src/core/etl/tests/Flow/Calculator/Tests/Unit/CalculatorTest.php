<?php

declare(strict_types=1);

namespace Flow\Calculator\Tests\Unit;

use Flow\Calculator\{Calculator, Rounding};
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class CalculatorTest extends TestCase
{
    #[TestWith(['1', '1', 2])]
    #[TestWith([1, 1, 2])]
    #[TestWith([1.0, 1.0, 2])]

    #[TestWith([1.1, 1.1, 2.2])]
    #[TestWith([1.1, 1.1, 2.2])]
    #[TestWith([1.1, '1.1', 2.2])]

    #[TestWith([1.123456, 1.123456, 2.246912])]
    #[TestWith(['3.23E-5', '3.23E-5', 0.0000646])]
    #[TestWith(['3.23E-5', '3.23e-5', 0.0000646])]
    #[TestWith([3.23E-5, '3.23e-5', 0.0000646])]
    public function test_add(string|int|float $a, string|int|float $b, int|float $output) : void
    {
        self::assertSame($output, (new Calculator())->add($a, $b));
    }

    #[TestWith(['1', '1', 1])]
    #[TestWith([1, 1, 1])]
    #[TestWith([1.0, 1.0, 1])]
    #[TestWith([1.1, 1.1, 1])]
    #[TestWith([1.1, 5, 0.2])]
    #[TestWith([1.12, 1.0, 1.12])]
    #[TestWith([1.123456, 1.123456, 1])]
    #[TestWith(['3.23E-5', '3.23E-5', 1])]
    #[TestWith(['3.23E-5', '3.23e-5', 1])]
    #[TestWith([3.23E-5, '3.23e-5', 1])]
    public function test_divide(string|int|float $a, string|int|float $b, int|float $output) : void
    {
        self::assertSame($output, (new Calculator())->divide($a, $b, rounding: Rounding::HALF_UP));
    }

    public function test_divide_by_zero() : void
    {
        $this->expectException(\DivisionByZeroError::class);
        $this->expectExceptionMessage('Division by zero');

        (new Calculator())->divide(1, 0);
    }

    public function test_divide_by_zero_as_float() : void
    {
        $this->expectException(\DivisionByZeroError::class);
        $this->expectExceptionMessage('Division by zero');

        (new Calculator())->divide(1, 0.0);
    }

    public function test_divide_by_zero_as_string() : void
    {
        $this->expectException(\DivisionByZeroError::class);
        $this->expectExceptionMessage('Division by zero');

        (new Calculator())->divide(1, '0');
    }

    #[TestWith(['1', '1', 0])]
    #[TestWith([17, 3, 2])]
    public function test_modulus(string|int $a, string|int $b, int|float $output) : void
    {
        self::assertSame($output, (new Calculator())->modulus($a, $b));
    }

    #[TestWith(['1', '1', 1])]
    #[TestWith([1, 1, 1])]
    #[TestWith([1.0, 1, 1])]
    #[TestWith([1.1, 2, 1.21])]
    #[TestWith([1.1, 5, 1.61051])]
    #[TestWith([1.12, 1, 1.12])]
    public function test_power(string|int|float $a, string|int $b, int|float $output) : void
    {
        self::assertSame($output, (new Calculator())->power($a, $b));
    }

    #[TestWith(['1', '1', 0, 0])]
    #[TestWith(['0.3', '0.1', 6, 0.2])]
    #[TestWith([1, 1, 0, 0])]
    #[TestWith([1.0, 1.0, 0, 0])]
    #[TestWith([1.1, 1.1, 0, 0])]
    #[TestWith([1.1, 1.1, 1, 0])]
    #[TestWith([1.1, '1.1', 1, 0])]
    #[TestWith([1.123456, 1.123456, 6, 0])]
    #[TestWith(['3.23E-5', '3.23E-5', 12, 0])]
    #[TestWith(['3.23E-5', '3.23e-5', 12, 0])]
    #[TestWith([3.23E-5, '3.23e-5', 12, 0])]
    public function test_subtract(string|int|float $a, string|int|float $b, int $scale, int|float $output) : void
    {
        self::assertSame($output, (new Calculator())->subtract($a, $b));
    }
}
