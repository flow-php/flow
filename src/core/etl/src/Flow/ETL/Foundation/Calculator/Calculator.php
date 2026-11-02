<?php

declare(strict_types=1);

namespace Flow\ETL\Foundation\Calculator;

use Brick\Math\BigDecimal;
use Brick\Math\BigInteger;
use Brick\Math\Exception\DivisionByZeroException;
use Brick\Math\Exception\RoundingNecessaryException;
use Brick\Math\RoundingMode;
use DivisionByZeroError;
use Flow\ETL\Foundation\Calculator\Exception\InvalidScaleException;
use Flow\ETL\Foundation\Calculator\Exception\NonNumericValueException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;

use function abs;
use function defined;
use function floor;
use function ini_get;
use function is_finite;
use function is_float;
use function is_int;
use function is_string;
use function method_exists;
use function min;
use function sprintf;

use const INF;
use const PHP_FLOAT_EPSILON;
use const PHP_FLOAT_MIN;

final class Calculator
{
    private const int FLOAT_EXACT_INT = 9_007_199_254_740_992;

    private const float FLOAT_QUOTIENT_MAX_INPUT_ERROR = 1.0e-6;

    private const float FLOAT_QUOTIENT_MAX_MAGNITUDE = 4_503_599_627_370_496.0;

    private const int FLOAT_QUOTIENT_MAX_SCALE = 15;

    /**
     * floor(sqrt(PHP_INT_MAX)): two ints no larger than this multiply natively without overflowing, so the product
     * is exact and needs no BigDecimal.
     */
    private const int OVERFLOW_FREE_FACTOR = 3_037_000_499;

    /**
     * @param float|int|numeric-string $a
     * @param float|int|numeric-string $b
     *
     * @throws NonNumericValueException
     * @throws InvalidScaleException
     */
    public function add(int|float|string $a, int|float|string $b): int|float
    {
        $result = BigDecimal::of((string) $a)->plus(BigDecimal::of((string) $b));

        if (self::eitherIsFloat($a, $b) || self::hasNonZeroFractionalPart($result)) {
            return $result->toFloat();
        }

        return $result->toInt();
    }

    /**
     * @param float|int|numeric-string $a
     * @param float|int|numeric-string $b
     *
     * @throws Exception\RoundingNecessaryException
     * @throws \DivisionByZeroError
     */
    public function divide(
        int|float|string $a,
        int|float|string $b,
        ?int $scale = null,
        ?Rounding $rounding = null,
    ): float {
        $quotient = self::roundedFloatQuotient($a, $b, $scale, $rounding);

        if ($quotient !== null) {
            return $quotient;
        }

        try {
            $aDecimal = BigDecimal::of((string) $a);
            $effectiveScale = $scale ?? $aDecimal->getScale();

            $useNewNaming = defined('Brick\Math\RoundingMode::Up');

            // @mago-ignore analysis:possibly-invalid-argument
            $result = $aDecimal->dividedBy(BigDecimal::of((string) $b), $effectiveScale, match ($rounding) {
                // @mago-ignore analysis:non-existent-class-constant
                Rounding::UP => $useNewNaming ? RoundingMode::Up : RoundingMode::UP,
                // @mago-ignore analysis:non-existent-class-constant
                Rounding::DOWN => $useNewNaming ? RoundingMode::Down : RoundingMode::DOWN,
                // @mago-ignore analysis:non-existent-class-constant
                Rounding::CEILING => $useNewNaming ? RoundingMode::Ceiling : RoundingMode::CEILING,
                // @mago-ignore analysis:non-existent-class-constant
                Rounding::FLOOR => $useNewNaming ? RoundingMode::Floor : RoundingMode::FLOOR,
                // @mago-ignore analysis:non-existent-class-constant
                Rounding::HALF_UP => $useNewNaming ? RoundingMode::HalfUp : RoundingMode::HALF_UP,
                // @mago-ignore analysis:non-existent-class-constant
                Rounding::HALF_DOWN => $useNewNaming ? RoundingMode::HalfDown : RoundingMode::HALF_DOWN,
                // @mago-ignore analysis:non-existent-class-constant
                Rounding::HALF_CEILING => $useNewNaming ? RoundingMode::HalfCeiling : RoundingMode::HALF_CEILING,
                // @mago-ignore analysis:non-existent-class-constant
                Rounding::HALF_FLOOR => $useNewNaming ? RoundingMode::HalfFloor : RoundingMode::HALF_FLOOR,
                // @mago-ignore analysis:non-existent-class-constant
                Rounding::HALF_EVEN => $useNewNaming ? RoundingMode::HalfEven : RoundingMode::HALF_EVEN,
                // @mago-ignore analysis:non-existent-class-constant
                default => $useNewNaming ? RoundingMode::Unnecessary : RoundingMode::UNNECESSARY,
            });

            return $result->toFloat();
        } catch (DivisionByZeroException $e) {
            throw new DivisionByZeroError('Division by zero.', (int) $e->getCode(), $e);
        } catch (RoundingNecessaryException $e) {
            throw new Exception\RoundingNecessaryException($e->getMessage(), (int) $e->getCode(), $e);
        }
    }

    /**
     * @param int|numeric-string $a
     * @param int|numeric-string $b
     *
     * @throws InvalidArgumentException when $b is negative
     */
    public function modulus(int|string $a, int|string $b): int
    {
        // ints under a positive modulus: PHP's % with the sign folded back, as BigInteger::mod() answers; a zero
        // modulus and numeric strings keep BigInteger's checks
        if (is_int($a) && is_int($b) && $b > 0) {
            $remainder = $a % $b;

            return $remainder < 0 ? $remainder + $b : $remainder;
        }

        $modulus = BigInteger::of($b);

        // refused here, not by BigInteger::mod(): brick/math below 0.15 answers a negative modulus instead of throwing
        if ($modulus->isNegative()) {
            throw new InvalidArgumentException(sprintf('Modulus must not be negative, got "%s".', $b));
        }

        return BigInteger::of($a)->mod($modulus)->toInt();
    }

    /**
     * @param float|int|numeric-string $a
     * @param float|int|numeric-string $b
     */
    public function multiply(int|float|string $a, int|float|string $b): int|float
    {
        if (
            is_int($a)
            && is_int($b)
            && abs($a) <= self::OVERFLOW_FREE_FACTOR
            && abs($b) <= self::OVERFLOW_FREE_FACTOR
        ) {
            return $a * $b;
        }

        $result = BigDecimal::of((string) $a)->multipliedBy(BigDecimal::of((string) $b));

        if (self::eitherIsFloat($a, $b) || self::hasNonZeroFractionalPart($result)) {
            return $result->toFloat();
        }

        return $result->toInt();
    }

    /**
     * @param float|int|numeric-string $a
     * @param int|numeric-string $b
     */
    public function power(int|float|string $a, int|string $b): int|float
    {
        // @mago-ignore analysis:possibly-invalid-argument
        $result = BigDecimal::of((string) $a)->power(BigInteger::of((string) $b)->toInt());

        if (self::eitherIsFloat($a, $b) || self::hasNonZeroFractionalPart($result)) {
            return $result->toFloat();
        }

        return $result->toInt();
    }

    /**
     * @param float|int|numeric-string $a
     * @param float|int|numeric-string $b
     */
    public function subtract(int|float|string $a, int|float|string $b): int|float
    {
        $result = BigDecimal::of((string) $a)->minus(BigDecimal::of((string) $b));

        if (self::eitherIsFloat($a, $b) || self::hasNonZeroFractionalPart($result)) {
            return $result->toFloat();
        }

        return $result->toInt();
    }

    /**
     * @return null|float the decimal division's result, or null when the float quotient lies too close to a rounding edge to decide it
     */
    private static function roundedFloatQuotient(
        int|float|string $a,
        int|float|string $b,
        ?int $scale,
        ?Rounding $rounding,
    ): ?float {
        if (
            $scale === null
            || $scale < 0
            || $scale > self::FLOAT_QUOTIENT_MAX_SCALE
            || $rounding === null
            || $rounding === Rounding::UNNECESSARY
            || is_string($a)
            || is_string($b)
        ) {
            return null;
        }

        $error = self::decimalFormError($a) + self::decimalFormError($b);
        $dividend = (float) $a;
        $divisor = (float) $b;

        if (
            $error > self::FLOAT_QUOTIENT_MAX_INPUT_ERROR
            || !is_finite($dividend)
            || !is_finite($divisor)
            || $divisor === 0.0
        ) {
            return null;
        }

        if ($dividend === 0.0) {
            return 0.0;
        }

        $quotient = $dividend / $divisor;
        $power = (float) 10 ** $scale;
        $magnitude = abs($quotient) * $power;

        if (abs($quotient) < PHP_FLOAT_MIN || $magnitude >= self::FLOAT_QUOTIENT_MAX_MAGNITUDE) {
            return null;
        }

        $window = $magnitude * ((2 * $error) + (8 * PHP_FLOAT_EPSILON));
        $whole = floor($magnitude);
        $fraction = $magnitude - $whole;
        $negative = $quotient < 0;
        $awayFromZero = match ($rounding) {
            Rounding::UP => true,
            Rounding::DOWN => false,
            Rounding::CEILING => !$negative,
            Rounding::FLOOR => $negative,
            // every half mode rounds to the nearest once the window rules out a tie
            default => null,
        };

        if ($awayFromZero === null) {
            if (abs($fraction - 0.5) <= $window) {
                return null;
            }

            $rounded = $fraction < 0.5 ? $whole : $whole + 1;
        } else {
            if ($fraction <= $window || (1 - $fraction) <= $window) {
                return null;
            }

            $rounded = $awayFromZero ? $whole + 1 : $whole;
        }

        if ($rounded === 0.0) {
            return 0.0;
        }

        return $negative ? -($rounded / $power) : $rounded / $power;
    }

    /**
     * @return float the relative error of (string) $value, the form BigDecimal::of() reads, against $value
     */
    private static function decimalFormError(int|float $value): float
    {
        if (is_int($value)) {
            return abs($value) <= self::FLOAT_EXACT_INT ? 0.0 : INF;
        }

        $precision = (int) ini_get('precision');

        return match (true) {
            $precision === -1 => PHP_FLOAT_EPSILON,
            $precision >= 1 => 5 * (10 ** (-min($precision, 17))),
            default => INF,
        };
    }

    private static function eitherIsFloat(int|float|string $a, int|float|string $b): bool
    {
        return self::isFloatOperand($a) || self::isFloatOperand($b);
    }

    private static function hasNonZeroFractionalPart(BigDecimal $result): bool
    {
        if (method_exists($result, 'hasNonZeroFractionalPart')) {
            // @mago-ignore analysis:mixed-return-statement
            return $result->hasNonZeroFractionalPart();
        }

        return !$result->getFractionalPart()->isZero();
    }

    private static function isFloatOperand(int|float|string $value): bool
    {
        // A numeric string carries no integer guarantee, so it counts as a float operand.
        return is_float($value) || is_string($value);
    }
}
