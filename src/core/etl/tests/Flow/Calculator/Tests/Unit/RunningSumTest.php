<?php

declare(strict_types=1);

namespace Flow\Calculator\Tests\Unit;

use Flow\Calculator\RunningSum;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function is_nan;
use function is_numeric;

use const INF;

final class RunningSumTest extends TestCase
{
    #[TestWith([0, 1, 1])]
    #[TestWith([10, 5, 15])]
    public function test_int_operands_stay_int(int $first, int $second, int $output): void
    {
        $sum = new RunningSum();
        $sum->add($first, false);
        $sum->add($second, false);

        static::assertSame($output, $sum->value());
    }

    #[TestWith([1.5, 1.5, 3.0])]
    #[TestWith([0, 2.5, 2.5])]
    #[TestWith([2.5, 0, 2.5])]
    public function test_float_operand_yields_float(float|int $first, float|int $second, float $output): void
    {
        $sum = new RunningSum();
        $sum->add($first, false);
        $sum->add($second, false);

        static::assertSame($output, $sum->value());
    }

    #[TestWith(['10', 10.0])]
    #[TestWith(['2.5', 2.5])]
    public function test_numeric_string_operand_yields_float(string $value, float $output): void
    {
        assert(is_numeric($value), 'String parameter $value must be numeric');

        $sum = new RunningSum();
        $sum->add($value, false);

        static::assertSame($output, $sum->value());
    }

    public function test_compensation_keeps_ten_tenths_at_one(): void
    {
        $sum = new RunningSum();

        for ($i = 0; $i < 10; $i++) {
            $sum->add(0.1, false);
        }

        // naive IEEE addition gives 0.9999999999999999
        static::assertSame(1.0, $sum->value());
    }

    public function test_compensation_keeps_a_small_value_next_to_large_ones(): void
    {
        $sum = new RunningSum();
        $sum->add(1e16, false);
        $sum->add(1.0, false);
        $sum->add(-1e16, false);

        // naive IEEE addition gives 0.0
        static::assertSame(1.0, $sum->value());
    }

    public function test_an_infinite_sum_stays_infinite(): void
    {
        $sum = new RunningSum();
        $sum->add(INF, false);
        $sum->add(1.5, false);

        static::assertSame(INF, $sum->value());
        static::assertFalse(is_nan($sum->value()));
    }

    public function test_exact_mode_avoids_floating_point_drift(): void
    {
        $sum = new RunningSum();
        $sum->add(0.1, true);
        $sum->add(0.2, true);

        static::assertSame(0.3, $sum->value());
    }

    public function test_exact_mode_keeps_int_operands_int(): void
    {
        $sum = new RunningSum();
        $sum->add(1, true);
        $sum->add(2, true);

        static::assertSame(3, $sum->value());
    }

    public function test_merge_carries_the_other_compensation(): void
    {
        $left = new RunningSum();
        $right = new RunningSum();

        for ($i = 0; $i < 5; $i++) {
            $left->add(0.1, false);
            $right->add(0.1, false);
        }

        $left->merge($right, false);

        static::assertSame(1.0, $left->value());
    }

    public function test_exact_merge_is_decimal(): void
    {
        $left = new RunningSum();
        $right = new RunningSum();
        $left->add(0.1, false);
        $right->add(0.2, false);

        $left->merge($right, true);

        static::assertSame(0.3, $left->value());
    }
}
