<?php

declare(strict_types=1);

namespace Flow\Calculator;

use Brick\Math\BigDecimal;
use Brick\Math\BigInteger;
use Brick\Math\Exception\DivisionByZeroException;
use Brick\Math\Exception\RoundingNecessaryException;
use Brick\Math\RoundingMode;
use DivisionByZeroError;
use Flow\Calculator\Exception\InvalidScaleException;
use Flow\Calculator\Exception\NonNumericValueException;

use function defined;
use function method_exists;

final class Calculator
{
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

        if (!self::hasNonZeroFractionalPart($result)) {
            return $result->toInt();
        }

        return $result->toFloat();
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
    ): int|float {
        try {
            $aDecimal = BigDecimal::of((string) $a);
            $effectiveScale = $scale ?? $aDecimal->getScale();

            $useNewNaming = defined('Brick\Math\RoundingMode::Up');

            $brickMode = match ($rounding) {
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
            };

            // @mago-ignore analysis:possibly-invalid-argument
            $result = $aDecimal->dividedBy(BigDecimal::of((string) $b), $effectiveScale, $brickMode);

            if (!self::hasNonZeroFractionalPart($result)) {
                return $result->toInt();
            }

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
     */
    public function modulus(int|string $a, int|string $b): int
    {
        return BigInteger::of($a)->mod(BigInteger::of($b))->toInt();
    }

    /**
     * @param float|int|numeric-string $a
     * @param float|int|numeric-string $b
     */
    public function multiply(int|float|string $a, int|float|string $b): int|float
    {
        $result = BigDecimal::of((string) $a)->multipliedBy(BigDecimal::of((string) $b));

        if (!self::hasNonZeroFractionalPart($result)) {
            return $result->toInt();
        }

        return $result->toFloat();
    }

    /**
     * @param float|int|numeric-string $a
     * @param int|numeric-string $b
     */
    public function power(int|float|string $a, int|string $b): int|float
    {
        // @mago-ignore analysis:possibly-invalid-argument
        $result = BigDecimal::of((string) $a)->power(BigInteger::of((string) $b)->toInt());

        if (!self::hasNonZeroFractionalPart($result)) {
            return $result->toInt();
        }

        return $result->toFloat();
    }

    /**
     * @param float|int|numeric-string $a
     * @param float|int|numeric-string $b
     */
    public function subtract(int|float|string $a, int|float|string $b): int|float
    {
        $result = BigDecimal::of((string) $a)->minus(BigDecimal::of((string) $b));

        if (!self::hasNonZeroFractionalPart($result)) {
            return $result->toInt();
        }

        return $result->toFloat();
    }

    private static function hasNonZeroFractionalPart(BigDecimal $result): bool
    {
        if (method_exists($result, 'hasNonZeroFractionalPart')) {
            // @mago-ignore analysis:mixed-return-statement
            return $result->hasNonZeroFractionalPart();
        }

        return !$result->getFractionalPart()->isZero();
    }
}
