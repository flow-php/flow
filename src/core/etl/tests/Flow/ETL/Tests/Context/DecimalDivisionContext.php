<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Brick\Math\BigDecimal;
use Brick\Math\RoundingMode;
use Flow\ETL\Foundation\Calculator\Calculator;
use Flow\ETL\Foundation\Calculator\Rounding;

use function defined;
use function pack;
use function sprintf;
use function unpack;
use function var_export;

final class DecimalDivisionContext
{
    public static function bigDecimalQuotientOfStringForms(
        float|int $a,
        float|int $b,
        int $scale,
        Rounding $rounding,
    ): float {
        $modern = defined('Brick\Math\RoundingMode::Up');

        // @mago-ignore analysis:possibly-invalid-argument
        return BigDecimal::of((string) $a)->dividedBy(BigDecimal::of((string) $b), $scale, match ($rounding) {
            // @mago-ignore analysis:non-existent-class-constant
            Rounding::UP => $modern ? RoundingMode::Up : RoundingMode::UP,
            // @mago-ignore analysis:non-existent-class-constant
            Rounding::DOWN => $modern ? RoundingMode::Down : RoundingMode::DOWN,
            // @mago-ignore analysis:non-existent-class-constant
            Rounding::CEILING => $modern ? RoundingMode::Ceiling : RoundingMode::CEILING,
            // @mago-ignore analysis:non-existent-class-constant
            Rounding::FLOOR => $modern ? RoundingMode::Floor : RoundingMode::FLOOR,
            // @mago-ignore analysis:non-existent-class-constant
            Rounding::HALF_UP => $modern ? RoundingMode::HalfUp : RoundingMode::HALF_UP,
            // @mago-ignore analysis:non-existent-class-constant
            Rounding::HALF_DOWN => $modern ? RoundingMode::HalfDown : RoundingMode::HALF_DOWN,
            // @mago-ignore analysis:non-existent-class-constant
            Rounding::HALF_CEILING => $modern ? RoundingMode::HalfCeiling : RoundingMode::HALF_CEILING,
            // @mago-ignore analysis:non-existent-class-constant
            Rounding::HALF_FLOOR => $modern ? RoundingMode::HalfFloor : RoundingMode::HALF_FLOOR,
            // @mago-ignore analysis:non-existent-class-constant
            Rounding::HALF_EVEN => $modern ? RoundingMode::HalfEven : RoundingMode::HALF_EVEN,
            // @mago-ignore analysis:non-existent-class-constant
            Rounding::UNNECESSARY => $modern ? RoundingMode::Unnecessary : RoundingMode::UNNECESSARY,
        })->toFloat();
    }

    /**
     * @param list<float|int> $dividends
     * @param list<float|int> $divisors
     * @param list<int> $scales
     *
     * @return list<string> each case where Calculator::divide() differs from the decimal division
     */
    public static function mismatches(array $dividends, array $divisors, array $scales): array
    {
        $calculator = new Calculator();
        $mismatches = [];

        foreach ($dividends as $a) {
            foreach ($divisors as $b) {
                foreach ($scales as $scale) {
                    foreach (Rounding::cases() as $rounding) {
                        if ($rounding === Rounding::UNNECESSARY) {
                            continue;
                        }

                        $mismatch = self::mismatch($calculator, $a, $b, $scale, $rounding);

                        if ($mismatch !== null) {
                            $mismatches[] = $mismatch;
                        }
                    }
                }
            }
        }

        return $mismatches;
    }

    public static function mismatch(
        Calculator $calculator,
        float|int $a,
        float|int $b,
        int $scale,
        Rounding $rounding,
    ): ?string {
        // var_export tells -0.0 from 0.0 and prints every digit of the double
        $expected = var_export(self::bigDecimalQuotientOfStringForms($a, $b, $scale, $rounding), true);
        $actual = var_export($calculator->divide($a, $b, $scale, $rounding), true);

        return $expected === $actual
            ? null
            : sprintf(
                '%s / %s at scale %d %s: decimal %s, divide %s',
                var_export($a, true),
                var_export($b, true),
                $scale,
                $rounding->name,
                $expected,
                $actual,
            );
    }

    public static function adjacent(float $value, int $direction): float
    {
        /** @var array{1: int} $bits */
        $bits = unpack('q', pack('d', $value));

        /** @var array{1: float} $float */
        $float = unpack('d', pack('q', $bits[1] + ($value >= 0 ? $direction : -$direction)));

        return $float[1];
    }
}
