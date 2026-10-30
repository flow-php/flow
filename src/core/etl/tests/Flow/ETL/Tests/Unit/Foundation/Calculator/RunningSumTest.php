<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Foundation\Calculator;

use Flow\ETL\Foundation\Calculator\RunningSum;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function is_nan;
use function is_numeric;

use const INF;

final class RunningSumTest extends TestCase
{
    public function test_empty_partial_is_the_partial_of_a_fresh_sum(): void
    {
        static::assertSame(RunningSum::emptyPartial(), (new RunningSum())->partial());
        static::assertSame(0, RunningSum::fromPartial(RunningSum::emptyPartial())->value());
    }

    public function test_partial_type_accepts_every_partial(): void
    {
        $sum = new RunningSum();
        $sum->add(1.5, false);

        static::assertSame($sum->partial(), RunningSum::partialType()->assert($sum->partial()));
        static::assertSame(RunningSum::emptyPartial(), RunningSum::partialType()->assert(RunningSum::emptyPartial()));
    }

    /**
     * @param list<float|int|numeric-string> $values
     */
    #[TestWith([[1, 2, 3], false])]
    #[TestWith([[0.1, 0.2, 0.3, 1e16, -1e16, 0.1], false])]
    #[TestWith([[1, 2.5, '3.25', 4], false])]
    #[TestWith([[9_223_372_036_854_775_807, 1, 2], false])]
    #[TestWith([[0.1, 0.2, 0.3], true])]
    #[TestWith([[], false])]
    public function test_add_all_equals_add_per_value(array $values, bool $exact): void
    {
        $perValue = new RunningSum();
        $batched = new RunningSum();

        foreach ($values as $value) {
            $perValue->add($value, $exact);
        }

        $batched->addAll($values, $exact);

        static::assertSame($perValue->partial(), $batched->partial());
        static::assertSame($perValue->value(), $batched->value());
    }

    public function test_add_all_continues_a_started_sum(): void
    {
        $perValue = new RunningSum();
        $batched = new RunningSum();

        foreach ([0.1, 0.2] as $value) {
            $perValue->add($value, false);
            $batched->add($value, false);
        }

        foreach ([0.3, 1e16, 0.4] as $value) {
            $perValue->add($value, false);
        }

        $batched->addAll([0.3, 1e16, 0.4], false);

        static::assertSame($perValue->partial(), $batched->partial());
    }

    public function test_an_int_partial_stays_exact_past_2_53(): void
    {
        $sum = new RunningSum();
        $sum->add(9_007_199_254_740_993, false);

        $partial = $sum->partial();

        static::assertSame(['int' => 9_007_199_254_740_993, 'float' => null, 'compensation' => 0.0], $partial);
        static::assertSame(9_007_199_254_740_993, RunningSum::fromPartial($partial)->value());
    }

    public function test_a_float_partial_keeps_its_compensation(): void
    {
        $sum = new RunningSum();

        foreach ([1.0, 1e-16, 1e-16] as $value) {
            $sum->add($value, false);
        }

        $partial = $sum->partial();
        $restored = RunningSum::fromPartial($partial);
        $restored->add(1e-16, false);

        static::assertNull($partial['int']);
        static::assertSame($sum->value(), RunningSum::fromPartial($partial)->value());
        static::assertSame($partial['compensation'], RunningSum::fromPartial($partial)->partial()['compensation']);
        static::assertSame(1.0000000000000002, $restored->value());
    }

    public function test_from_partial_refuses_a_partial_with_both_sums(): void
    {
        $this->expectException(InvalidArgumentException::class);

        RunningSum::fromPartial(['int' => 1, 'float' => 1.0, 'compensation' => 0.0]);
    }

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
