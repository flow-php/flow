<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Foundation\Calculator;

use Brick\Math\Exception\IntegerOverflowException;
use Brick\Math\Exception\MathException;
use DivisionByZeroError;
use Flow\ETL\Foundation\Calculator\Calculator;
use Flow\ETL\Foundation\Calculator\Rounding;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class CalculatorTest extends TestCase
{
    public function test_modulus_refuses_a_zero_or_negative_modulus_for_ints_as_for_strings(): void
    {
        $calculator = new Calculator();

        static::assertSame(1, $calculator->modulus(7, 3));

        $this->expectException(MathException::class);

        $calculator->modulus(7, -3);
    }

    public function test_modulus_by_zero_is_refused_for_ints(): void
    {
        $this->expectException(MathException::class);

        (new Calculator())->modulus(7, 0);
    }

    #[TestWith(['1', '1', 2.0])]
    #[TestWith([1, 1, 2])]
    #[TestWith([1.0, 1.0, 2.0])]
    #[TestWith([1.1, 1.1, 2.2])]
    #[TestWith([1.1, '1.1', 2.2])]
    #[TestWith([1.123456, 1.123456, 2.246912])]
    #[TestWith(['3.23E-5', '3.23E-5', 0.0000646])]
    #[TestWith(['3.23E-5', '3.23e-5', 0.0000646])]
    #[TestWith([3.23E-5, '3.23e-5', 0.0000646])]
    /**
     * @param float|int|numeric-string $a
     * @param float|int|numeric-string $b
     * @param float|int $output
     */
    public function test_add(string|int|float $a, string|int|float $b, int|float $output): void
    {
        if (is_string($a)) {
            assert(is_numeric($a), 'String parameter $a must be numeric');
        }

        if (is_string($b)) {
            assert(is_numeric($b), 'String parameter $b must be numeric');
        }

        static::assertSame($output, (new Calculator())->add($a, $b));
    }

    #[TestWith(['1', '1', 1.0])]
    #[TestWith([1, 1, 1.0])]
    #[TestWith([1.0, 1.0, 1.0])]
    #[TestWith([1.1, 1.1, 1.0])]
    #[TestWith([1.1, 5, 0.2])]
    #[TestWith([1.12, 1.0, 1.12])]
    #[TestWith([1.123456, 1.123456, 1.0])]
    #[TestWith(['3.23E-5', '3.23E-5', 1.0])]
    #[TestWith(['3.23E-5', '3.23e-5', 1.0])]
    #[TestWith([3.23E-5, '3.23e-5', 1.0])]
    /**
     * @param float|int|numeric-string $a
     * @param float|int|numeric-string $b
     * @param float|int $output
     */
    public function test_divide(string|int|float $a, string|int|float $b, int|float $output): void
    {
        if (is_string($a)) {
            assert(is_numeric($a), 'String parameter $a must be numeric');
        }

        if (is_string($b)) {
            assert(is_numeric($b), 'String parameter $b must be numeric');
        }

        static::assertSame($output, (new Calculator())->divide($a, $b, rounding: Rounding::HALF_UP));
    }

    #[TestWith([4, 2, 2.0])]
    #[TestWith([4.0, 2, 2.0])]
    public function test_divide_always_yields_float(int|float $a, int|float $b, float $output): void
    {
        static::assertSame($output, (new Calculator())->divide($a, $b));
    }

    public function test_divide_by_zero(): void
    {
        $this->expectException(DivisionByZeroError::class);
        $this->expectExceptionMessage('Division by zero');

        (new Calculator())->divide(1, 0);
    }

    public function test_divide_by_zero_as_float(): void
    {
        $this->expectException(DivisionByZeroError::class);
        $this->expectExceptionMessage('Division by zero');

        (new Calculator())->divide(1, 0.0);
    }

    public function test_divide_by_zero_as_string(): void
    {
        $this->expectException(DivisionByZeroError::class);
        $this->expectExceptionMessage('Division by zero');

        (new Calculator())->divide(1, '0');
    }

    public function test_float_operand_always_yields_float(): void
    {
        $calculator = new Calculator();

        static::assertSame(2.0, $calculator->add(1.0, 1));
        static::assertSame(0.0, $calculator->subtract(1, 1.0));
        static::assertSame(3.0, $calculator->multiply(1.5, 2));
        static::assertSame(1.0, $calculator->power(1.0, 3));
    }

    public function test_multiply_keeps_the_largest_overflow_free_int_products_exact(): void
    {
        $calculator = new Calculator();

        static::assertSame(9_223_372_030_926_249_001, $calculator->multiply(3_037_000_499, 3_037_000_499));
        static::assertSame(-9_223_372_030_926_249_001, $calculator->multiply(-3_037_000_499, 3_037_000_499));
    }

    public function test_multiply_refuses_an_int_product_that_overflows(): void
    {
        $this->expectException(IntegerOverflowException::class);

        (new Calculator())->multiply(3_037_000_500, 3_037_000_500);
    }

    public function test_int_operands_yield_int(): void
    {
        $calculator = new Calculator();

        static::assertSame(3, $calculator->add(1, 2));
        static::assertSame(2, $calculator->subtract(5, 3));
        static::assertSame(12, $calculator->multiply(3, 4));
        static::assertSame(8, $calculator->power(2, 3));
    }

    #[TestWith(['1', '1', 0])]
    #[TestWith([17, 3, 2])]
    #[TestWith([-7, 3, 2])]
    #[TestWith(['-7', 3, 2])]
    #[TestWith([-9, 3, 0])]
    #[TestWith([PHP_INT_MIN, 7, 6])]
    /**
     * @param int|numeric-string $a
     * @param int|numeric-string $b
     * @param float|int $output
     */
    public function test_modulus(string|int $a, string|int $b, int|float $output): void
    {
        if (is_string($a)) {
            assert(is_numeric($a), 'String parameter $a must be numeric');
        }

        if (is_string($b)) {
            assert(is_numeric($b), 'String parameter $b must be numeric');
        }

        static::assertSame($output, (new Calculator())->modulus($a, $b));
    }

    #[TestWith(['2', '3', 6.0])]
    #[TestWith([2, 3, 6])]
    #[TestWith([2.0, 3, 6.0])]
    #[TestWith([1.1, 2, 2.2])]
    #[TestWith([0.3, 1, 0.3])]
    /**
     * @param float|int|numeric-string $a
     * @param float|int|numeric-string $b
     */
    public function test_multiply(string|int|float $a, string|int|float $b, int|float $output): void
    {
        if (is_string($a)) {
            assert(is_numeric($a), 'String parameter $a must be numeric');
        }

        if (is_string($b)) {
            assert(is_numeric($b), 'String parameter $b must be numeric');
        }

        static::assertSame($output, (new Calculator())->multiply($a, $b));
    }

    #[TestWith(['1', '1', 1.0])]
    #[TestWith([1, 1, 1])]
    #[TestWith([1.0, 1, 1.0])]
    #[TestWith([1.1, 2, 1.21])]
    #[TestWith([1.1, 5, 1.61051])]
    #[TestWith([1.12, 1, 1.12])]
    /**
     * @param float|int|numeric-string $a
     * @param int|numeric-string $b
     * @param float|int $output
     */
    public function test_power(string|int|float $a, string|int $b, int|float $output): void
    {
        if (is_string($a)) {
            assert(is_numeric($a), 'String parameter $a must be numeric');
        }

        if (is_string($b)) {
            assert(is_numeric($b), 'String parameter $b must be numeric');
        }

        static::assertSame($output, (new Calculator())->power($a, $b));
    }

    #[TestWith(['1', '1', 0.0])]
    #[TestWith(['0.3', '0.1', 0.2])]
    #[TestWith([1, 1, 0])]
    #[TestWith([1.0, 1.0, 0.0])]
    #[TestWith([1.1, 1.1, 0.0])]
    #[TestWith([1.1, '1.1', 0.0])]
    #[TestWith([1.123456, 1.123456, 0.0])]
    #[TestWith(['3.23E-5', '3.23E-5', 0.0])]
    #[TestWith(['3.23E-5', '3.23e-5', 0.0])]
    #[TestWith([3.23E-5, '3.23e-5', 0.0])]
    /**
     * @param float|int|numeric-string $a
     * @param float|int|numeric-string $b
     * @param float|int $output
     */
    public function test_subtract(string|int|float $a, string|int|float $b, int|float $output): void
    {
        if (is_string($a)) {
            assert(is_numeric($a), 'String parameter $a must be numeric');
        }

        if (is_string($b)) {
            assert(is_numeric($b), 'String parameter $b must be numeric');
        }

        static::assertSame($output, (new Calculator())->subtract($a, $b));
    }
}
