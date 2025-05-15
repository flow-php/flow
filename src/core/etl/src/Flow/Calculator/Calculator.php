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
        $result = BigDecimal::of($a)->plus(BigDecimal::of($b));

        if (\in_array(\rtrim($result->getFractionalPart(), '0'), ['0', ''], true)) {
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
                $result = BigDecimal::of($a)->exactlyDividedBy(BigDecimal::of($b));

                if (\in_array(\rtrim($result->getFractionalPart(), '0'), ['0', ''], true)) {
                    return $result->toInt();
                }
            }

            if ($rounding === null) {
                $brickMode = RoundingMode::UNNECESSARY;
            } else {

                $brickMode = match ($rounding) {
                    Rounding::UNNECESSARY => RoundingMode::UNNECESSARY,
                    Rounding::UP => RoundingMode::UP,
                    Rounding::DOWN => RoundingMode::DOWN,
                    Rounding::CEILING => RoundingMode::CEILING,
                    Rounding::FLOOR => RoundingMode::FLOOR,
                    Rounding::HALF_UP => RoundingMode::HALF_UP,
                    Rounding::HALF_DOWN => RoundingMode::HALF_DOWN,
                    Rounding::HALF_CEILING => RoundingMode::HALF_CEILING,
                    Rounding::HALF_FLOOR => RoundingMode::HALF_FLOOR,
                    Rounding::HALF_EVEN => RoundingMode::HALF_EVEN,
                };
            }

            $result = BigDecimal::of($a)->dividedBy(BigDecimal::of($b), $scale, $brickMode);

            if (\in_array(\rtrim($result->getFractionalPart(), '0'), ['0', ''], true)) {
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
        $result = BigDecimal::of($a)->multipliedBy(BigDecimal::of($b));

        if (\in_array(\rtrim($result->getFractionalPart(), '0'), ['0', ''], true)) {
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
        $result = BigDecimal::of($a)->power(BigInteger::of($b)->toInt());

        if (\in_array($result->getFractionalPart(), ['0', ''], true)) {
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
        $result = BigDecimal::of($a)->minus(BigDecimal::of($b));

        if (\in_array(\rtrim($result->getFractionalPart(), '0'), ['0', ''], true)) {
            return $result->toInt();
        }

        return $result->toFloat();
    }
}
