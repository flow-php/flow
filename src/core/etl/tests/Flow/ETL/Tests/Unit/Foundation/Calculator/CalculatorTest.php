<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Foundation\Calculator;

use Brick\Math\Exception\IntegerOverflowException;
use Brick\Math\Exception\MathException;
use DivisionByZeroError;
use Flow\ETL\Foundation\Calculator\Calculator;
use Flow\ETL\Foundation\Calculator\Rounding;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\DecimalDivisionContext;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function array_filter;
use function array_values;
use function count;
use function ini_get;
use function ini_set;
use function mt_rand;
use function mt_srand;

final class CalculatorTest extends TestCase
{
    /**
     * @param int|numeric-string $a
     * @param int|numeric-string $b
     */
    #[TestWith([7, -3])]
    #[TestWith(['7', '-3'])]
    public function test_modulus_refuses_a_negative_modulus_for_ints_as_for_strings(int|string $a, int|string $b): void
    {
        $calculator = new Calculator();

        static::assertSame(1, $calculator->modulus(7, 3));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Modulus must not be negative, got "-3".');

        $calculator->modulus($a, $b);
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

    public function test_divide_answers_the_decimal_division_at_every_rounding_edge(): void
    {
        $dividends = [0, 0.0, -0.0, 1, -1, 7, 1.005, -1.005, 1.015, 2.675, 0.125, 0.5, 1.5, 2.5, -2.5, 1e-7, 5e-324];
        $dividends = [...$dividends, 123_456_789.125, 1e15, 9_007_199_254_740_991, -9_007_199_254_740_991];
        $dividends = [...$dividends, 9_007_199_254_740_992, 9_007_199_254_740_993, 9.007199254740992e15, 1e300];

        foreach ([1.005, 2.675, 0.125, 1.25e-5, 12.345, -12.345, 0.5, 1e-7] as $edge) {
            $dividends[] = DecimalDivisionContext::adjacent($edge, 1);
            $dividends[] = DecimalDivisionContext::adjacent($edge, -1);
        }

        static::assertSame(
            [],
            DecimalDivisionContext::mismatches($dividends, [1, 3, 7, -3, 1_000_000_007, 0.1], [0, 2, 6]),
        );
    }

    public function test_divide_answers_the_decimal_division_over_a_seeded_sweep(): void
    {
        mt_srand(2026);
        $calculator = new Calculator();
        $roundings = array_values(array_filter(
            Rounding::cases(),
            static fn(Rounding $r): bool => $r !== Rounding::UNNECESSARY,
        ));
        $mismatches = [];

        for ($case = 0; $case < 1_000; $case++) {
            $decimals = mt_rand(0, 6);
            $a = match ($case % 3) {
                0 => mt_rand(-1_000_000_000, 1_000_000_000) / (10 ** $decimals),
                1 => mt_rand(-1_000_000, 1_000_000),
                default => (mt_rand(-1_000_000, 1_000_000) + 0.5) / (10 ** $decimals),
            };
            $mismatch = DecimalDivisionContext::mismatch(
                $calculator,
                $a,
                mt_rand(1, 3) === 1 ? mt_rand(1, 10) : mt_rand(1, 100_000),
                mt_rand(0, 6),
                $roundings[mt_rand(0, count($roundings) - 1)],
            );

            if ($mismatch !== null) {
                $mismatches[] = $mismatch;
            }
        }

        mt_srand();

        static::assertSame([], $mismatches);
    }

    public function test_divide_answers_the_decimal_division_under_every_float_precision(): void
    {
        $precision = ini_get('precision');

        try {
            foreach (['-1', '17', '3', '1'] as $setting) {
                ini_set('precision', $setting);

                static::assertSame(
                    [],
                    DecimalDivisionContext::mismatches([1.005, 2.675, 123.456, -0.125], [1, 3, 7], [0, 2, 4]),
                );
            }
        } finally {
            ini_set('precision', (string) $precision);
        }
    }
}
