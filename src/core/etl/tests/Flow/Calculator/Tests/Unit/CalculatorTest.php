<?php

declare(strict_types=1);

namespace Flow\Calculator\Tests\Unit;

use Flow\Calculator\{Calculator, NumberNormalizer};
use Flow\Calculator\Exception\InvalidExponentException;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class CalculatorTest extends TestCase
{
    #[TestWith(['1', '1', 0, 2])]
    #[TestWith([1, 1, 0, 2])]
    #[TestWith([1.0, 1.0, 0, 2])]

    #[TestWith([1.1, 1.1, 0, 2])]
    #[TestWith([1.1, 1.1, 1, 2.2])]
    #[TestWith([1.1, '1.1', 1, 2.2])]

    #[TestWith([1.123456, 1.123456, 6, 2.246912])]
    #[TestWith(['3.23E-5', '3.23E-5', 12, 0.0000646])]
    #[TestWith(['3.23E-5', '3.23e-5', 12, 0.0000646])]
    #[TestWith([3.23E-5, '3.23e-5', 12, 0.0000646])]
    public function test_add(string|int|float $a, string|int|float $b, int $scale, int|float $output) : void
    {
        self::assertSame($output, (new Calculator())->add($a, $b, $scale));
    }

    #[TestWith(['1', '1', 0, 1])]
    #[TestWith([1, 1, 0, 1])]
    #[TestWith([1.0, 1.0, 0, 1])]
    #[TestWith([1.1, 1.1, 0, 1])]
    #[TestWith([1.1, 5, 2, 0.22])]
    #[TestWith([1.12, 1.0, 2, 1.12])]
    #[TestWith([1.123456, 1.123456, 6, 1])]
    #[TestWith(['3.23E-5', '3.23E-5', 12, 1])]
    #[TestWith(['3.23E-5', '3.23e-5', 12, 1])]
    #[TestWith([3.23E-5, '3.23e-5', 12, 1])]
    public function test_divide(string|int|float $a, string|int|float $b, int $scale, int|float $output) : void
    {
        self::assertSame($output, (new Calculator())->divide($a, $b, $scale));
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

    #[TestWith(['1', '1', 0, 0])]
    #[TestWith([1, 1, 0, 0])]
    #[TestWith([1.0, 1.0, 0, 0])]
    #[TestWith([1.1, 1.1, 0, 0])]
    #[TestWith([5.7, 1.3, 1, 0.5])]
    public function test_modulus(string|int|float $a, string|int|float $b, int $scale, int|float $output) : void
    {
        self::assertSame($output, (new Calculator())->modulus($a, $b, $scale));
    }

    #[TestWith(['1', '1', 0, 1])]
    #[TestWith([1, 1, 0, 1])]
    #[TestWith([1.0, 1.0, 0, 1])]
    #[TestWith([1.1, 1.1, 0, 1])]
    #[TestWith([1.1, 5, 2, 1.61])]
    #[TestWith([1.12, 1.0, 2, 1.12])]
    public function test_power(string|int|float $a, string|int|float $b, int $scale, int|float $output) : void
    {
        self::assertSame($output, (new Calculator())->power($a, $b, $scale));
    }

    #[TestWith([1.1, 1.1, 2])]
    #[TestWith([1.1, '1.1', 2])]
    public function test_power_with_exponent_as_float(string|int|float $a, string|int|float $b, int $scale) : void
    {
        $this->expectException(InvalidExponentException::class);
        $this->expectExceptionMessage('Exponent "' . NumberNormalizer::toString($b, $scale) . '" is invalid. It must be an integer.');

        (new Calculator())->power($a, $b, $scale);
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
        self::assertSame($output, (new Calculator())->subtract($a, $b, $scale));
    }
}
