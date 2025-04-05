<?php

declare(strict_types=1);

namespace Flow\Calculator;

use Flow\Calculator\Exception\{InvalidExponentException, InvalidScaleException, NonNumericValueException};

final class Calculator
{
    /**
     * @param float|int|numeric-string $a
     * @param float|int|numeric-string $b
     *
     * @throws NonNumericValueException
     * @throws InvalidScaleException
     */
    public function add(int|float|string $a, int|float|string $b, int $scale = 0) : int|float
    {
        return NumberNormalizer::toNumber(bcadd(NumberNormalizer::toString($a, $scale), NumberNormalizer::toString($b, $scale), $scale));
    }

    /**
     * @param float|int|numeric-string $a
     * @param float|int|numeric-string $b
     */
    public function divide(int|float|string $a, int|float|string $b, int $scale = 0) : int|float
    {
        if ($b === 0) {
            throw new \DivisionByZeroError('Division by zero');
        }

        return NumberNormalizer::toNumber(bcdiv(NumberNormalizer::toString($a, $scale), NumberNormalizer::toString($b, $scale), $scale));
    }

    /**
     * @param float|int|numeric-string $a
     * @param float|int|numeric-string $b
     */
    public function modulus(int|float|string $a, int|float|string $b, int $scale = 0) : int|float
    {
        return NumberNormalizer::toNumber(bcmod(NumberNormalizer::toString($a, $scale), NumberNormalizer::toString($b, $scale), $scale));
    }

    /**
     * @param float|int|numeric-string $a
     * @param float|int|numeric-string $b
     */
    public function multiply(int|float|string $a, int|float|string $b, int $scale = 0) : int|float
    {
        return NumberNormalizer::toNumber(bcmul(NumberNormalizer::toString($a, $scale), NumberNormalizer::toString($b, $scale), $scale));
    }

    /**
     * @param float|int|numeric-string $a
     * @param float|int|numeric-string $b
     */
    public function power(int|float|string $a, int|float|string $b, int $scale = 0) : int|float
    {
        $exponent = NumberNormalizer::toString($b, $scale);

        if (\str_contains($exponent, '.')) {
            throw new InvalidExponentException($exponent);
        }

        return NumberNormalizer::toNumber(bcpow(NumberNormalizer::toString($a, $scale), $exponent, $scale));
    }

    /**
     * @param float|int|numeric-string $a
     * @param float|int|numeric-string $b
     */
    public function subtract(int|float|string $a, int|float|string $b, int $scale = 0) : int|float
    {
        return NumberNormalizer::toNumber(bcsub(NumberNormalizer::toString($a, $scale), NumberNormalizer::toString($b, $scale), $scale));
    }
}
