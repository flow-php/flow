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

        if (!$result->hasNonZeroFractionalPart()) {
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
            if ($scale === null && $rounding === null) {
                $result = BigDecimal::of($a)->dividedByExact(BigDecimal::of($b));

                if (!$result->hasNonZeroFractionalPart()) {
                    return $result->toInt();
                }
            }

            $brickMode = match ($rounding) {
                Rounding::UP => RoundingMode::Up,
                Rounding::DOWN => RoundingMode::Down,
                Rounding::CEILING => RoundingMode::Ceiling,
                Rounding::FLOOR => RoundingMode::Floor,
                Rounding::HALF_UP => RoundingMode::HalfUp,
                Rounding::HALF_DOWN => RoundingMode::HalfDown,
                Rounding::HALF_CEILING => RoundingMode::HalfCeiling,
                Rounding::HALF_FLOOR => RoundingMode::HalfFloor,
                Rounding::HALF_EVEN => RoundingMode::HalfEven,
                default => RoundingMode::Unnecessary,
            };

            $result = BigDecimal::of((string) $a)->dividedBy(BigDecimal::of((string) $b), $scale, $brickMode);

            if (!$result->hasNonZeroFractionalPart()) {
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

        if (!$result->hasNonZeroFractionalPart()) {
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

        if (!$result->hasNonZeroFractionalPart()) {
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

        if (!$result->hasNonZeroFractionalPart()) {
            return $result->toInt();
        }

        return $result->toFloat();
    }
}
