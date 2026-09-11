<?php

declare(strict_types=1);

namespace Flow\Calculator\Tests\Unit;

use Flow\Calculator\RunningSum;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function is_numeric;

final class RunningSumTest extends TestCase
{
    #[TestWith([0, 1, 1])]
    #[TestWith([10, 5, 15])]
    public function test_int_operands_stay_int(int $sum, int $value, int $output): void
    {
        static::assertSame($output, (new RunningSum())->add($sum, $value, false));
    }

    #[TestWith([1.5, 1.5, 3.0])]
    #[TestWith([0, 2.5, 2.5])]
    #[TestWith([2.5, 0, 2.5])]
    public function test_float_operand_yields_float(float|int $sum, float|int $value, float $output): void
    {
        static::assertSame($output, (new RunningSum())->add($sum, $value, false));
    }

    #[TestWith(['10', 10.0])]
    #[TestWith(['2.5', 2.5])]
    public function test_numeric_string_operand_yields_float(string $value, float $output): void
    {
        assert(is_numeric($value), 'String parameter $value must be numeric');

        static::assertSame($output, (new RunningSum())->add(0, $value, false));
    }

    public function test_exact_mode_avoids_floating_point_drift(): void
    {
        static::assertSame(0.3, (new RunningSum())->add(0.1, 0.2, true));
    }

    public function test_inexact_mode_keeps_native_addition(): void
    {
        static::assertSame(0.1 + 0.2, (new RunningSum())->add(0.1, 0.2, false));
    }

    public function test_exact_mode_keeps_int_operands_int(): void
    {
        static::assertSame(3, (new RunningSum())->add(1, 2, true));
    }
}
