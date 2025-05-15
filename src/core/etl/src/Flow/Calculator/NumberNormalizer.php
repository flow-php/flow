<?php

declare(strict_types=1);

namespace Flow\Calculator;

use Flow\Calculator\Exception\{InvalidScaleException, NonNumericValueException};

final class NumberNormalizer
{
    /**
     * @param numeric-string $number
     *
     * @throws NonNumericValueException
     *
     * @return float|int
     */
    public static function toNumber(string $number) : float|int
    {
        if (!\is_numeric($number)) {
            throw new NonNumericValueException((string) $number);
        }

        if (\str_contains($number, '.')) {
            $number = \rtrim(\rtrim($number, '0'), '.');
        }

        if (\str_contains($number, '.')) {
            return (float) $number;
        }

        return (int) $number;
    }

    /**
     * @param float|int|numeric-string $number
     *
     * @return numeric-string
     */
    public static function toString(string|float|int $number, int $scale) : string
    {
        if ($scale < 0 || $scale > 16) {
            throw new InvalidScaleException($scale);
        }

        if (!\is_numeric($number)) {
            throw new NonNumericValueException((string) $number);
        }

        if (\is_string($number)) {
            // detect if scientific notation

            if (\str_contains($number, 'E') || \str_contains($number, 'e')) {
                $number = sprintf('%.' . $scale . 'F', $number);
            }

            if (\str_contains($number, '.')) {
                $number = \rtrim(\rtrim($number, '0'), '.');
            }

            /** @var numeric-string $number */
            return $number;
        }

        if (\is_int($number)) {
            return (string) $number;
        }

        $number = sprintf('%.' . $scale . 'F', $number);

        if (\str_contains($number, '.')) {
            $number = \rtrim(\rtrim($number, '0'), '.');
        }

        /** @var numeric-string $number */
        return $number;
    }
}
