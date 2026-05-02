<?php

declare(strict_types=1);

namespace Flow\Calculator;

use Brick\Math\{BigDecimal, BigInteger, Exception\RoundingNecessaryException, RoundingMode};
use Brick\Math\Exception\DivisionByZeroException;
use Flow\Calculator\Exception\{InvalidScaleException, NonNumericValueException};

final class Calculator
{
    /**
     * @param float|int|numeric-string $a
     * @param float|int|numeric-string $b
     *
     * @throws NonNumericValueException
     * @throws InvalidScaleException
     */
    public function add(int|float|string $a, int|float|string $b) : int|float
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
    public function divide(int|float|string $a, int|float|string $b, ?int $scale = null, ?Rounding $rounding = null) : int|float
    {
        try {
            $aDecimal = BigDecimal::of((string) $a);
            $effectiveScale = $scale ?? $aDecimal->getScale();

            $useNewNaming = \defined('Brick\Math\RoundingMode::Up');

            $brickMode = match ($rounding) {
                Rounding::UP => $useNewNaming ? RoundingMode::Up : RoundingMode::UP,
                Rounding::DOWN => $useNewNaming ? RoundingMode::Down : RoundingMode::DOWN,
                Rounding::CEILING => $useNewNaming ? RoundingMode::Ceiling : RoundingMode::CEILING,
                Rounding::FLOOR => $useNewNaming ? RoundingMode::Floor : RoundingMode::FLOOR,
                Rounding::HALF_UP => $useNewNaming ? RoundingMode::HalfUp : RoundingMode::HALF_UP,
                Rounding::HALF_DOWN => $useNewNaming ? RoundingMode::HalfDown : RoundingMode::HALF_DOWN,
                Rounding::HALF_CEILING => $useNewNaming ? RoundingMode::HalfCeiling : RoundingMode::HALF_CEILING,
                Rounding::HALF_FLOOR => $useNewNaming ? RoundingMode::HalfFloor : RoundingMode::HALF_FLOOR,
                Rounding::HALF_EVEN => $useNewNaming ? RoundingMode::HalfEven : RoundingMode::HALF_EVEN,
                default => $useNewNaming ? RoundingMode::Unnecessary : RoundingMode::UNNECESSARY,
            };

            $result = $aDecimal->dividedBy(BigDecimal::of((string) $b), $effectiveScale, $brickMode);

            if (!self::hasNonZeroFractionalPart($result)) {
                return $result->toInt();
            }

            return $result->toFloat();
        } catch (DivisionByZeroException $e) {
            throw new \DivisionByZeroError('Division by zero.', $e->getCode(), $e);
        } catch (RoundingNecessaryException $e) {
            throw new Exception\RoundingNecessaryException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @param int|numeric-string $a
     * @param int|numeric-string $b
     */
    public function modulus(int|string $a, int|string $b) : int
    {
        return BigInteger::of($a)->mod(BigInteger::of($b))->toInt();
    }

    /**
     * @param float|int|numeric-string $a
     * @param float|int|numeric-string $b
     */
    public function multiply(int|float|string $a, int|float|string $b) : int|float
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
    public function power(int|float|string $a, int|string $b) : int|float
    {
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
    public function subtract(int|float|string $a, int|float|string $b) : int|float
    {
        $result = BigDecimal::of((string) $a)->minus(BigDecimal::of((string) $b));

        if (!self::hasNonZeroFractionalPart($result)) {
            return $result->toInt();
        }

        return $result->toFloat();
    }

    private static function hasNonZeroFractionalPart(BigDecimal $result) : bool
    {
        if (\method_exists($result, 'hasNonZeroFractionalPart')) {
            return $result->hasNonZeroFractionalPart();
        }

        return !$result->getFractionalPart()->isZero();
    }
}
