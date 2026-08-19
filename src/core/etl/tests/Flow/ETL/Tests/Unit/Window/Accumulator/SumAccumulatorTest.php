<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Window\Accumulator;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ExecutionMode;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Window\Accumulator\SumAccumulator;

use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class SumAccumulatorTest extends FlowTestCase
{
    public function test_empty_frame_sums_to_null(): void
    {
        static::assertNull((new SumAccumulator(ref('value'), false, flow_context()))->value());
    }

    public function test_exact_as_scalar_function_is_resolved_per_row(): void
    {
        $accumulator = new SumAccumulator(ref('value'), lit(true), flow_context());

        $accumulator->accumulate(row(float_entry('value', 0.1)));
        $accumulator->accumulate(row(float_entry('value', 0.2)));

        static::assertSame(0.3, $accumulator->value());
    }

    public function test_exact_sum_avoids_floating_point_drift(): void
    {
        $accumulator = new SumAccumulator(ref('value'), true, flow_context());

        $accumulator->accumulate(row(float_entry('value', 0.1)));
        $accumulator->accumulate(row(float_entry('value', 0.2)));

        static::assertSame(0.3, $accumulator->value());
    }

    public function test_inexact_sum_keeps_native_addition(): void
    {
        $accumulator = new SumAccumulator(ref('value'), false, flow_context());

        $accumulator->accumulate(row(float_entry('value', 0.1)));
        $accumulator->accumulate(row(float_entry('value', 0.2)));

        static::assertSame(0.1 + 0.2, $accumulator->value());
    }

    public function test_missing_entry_is_reported_in_lenient_mode(): void
    {
        $context = flow_context();
        $context->functions()->setMode(ExecutionMode::LENIENT);

        $accumulator = new SumAccumulator(ref('value'), false, $context);
        $accumulator->accumulate(row(str_entry('other', 'x')));
        $accumulator->accumulate(row(int_entry('value', 5)));

        static::assertSame(5, $accumulator->value());
    }

    public function test_missing_entry_throws_in_strict_mode(): void
    {
        $context = flow_context();
        $context->functions()->setMode(ExecutionMode::STRICT);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/^Sum window function error: /');

        (new SumAccumulator(ref('value'), false, $context))->accumulate(row(str_entry('other', 'x')));
    }

    public function test_non_numeric_values_are_skipped(): void
    {
        $accumulator = new SumAccumulator(ref('value'), false, flow_context());

        $accumulator->accumulate(row(int_entry('value', 10)));
        $accumulator->accumulate(row(str_entry('value', 'not a number')));
        $accumulator->accumulate(row(int_entry('value', null)));
        $accumulator->accumulate(row(int_entry('value', 5)));

        static::assertSame(15, $accumulator->value());
    }

    public function test_numeric_strings_are_summed(): void
    {
        $accumulator = new SumAccumulator(ref('value'), false, flow_context());

        $accumulator->accumulate(row(str_entry('value', '10')));
        $accumulator->accumulate(row(str_entry('value', '2.5')));

        static::assertSame(12.5, $accumulator->value());
    }

    public function test_only_non_numeric_values_still_sums_to_null(): void
    {
        $accumulator = new SumAccumulator(ref('value'), false, flow_context());

        $accumulator->accumulate(row(int_entry('value', null)));

        static::assertNull($accumulator->value());
    }

    public function test_value_is_idempotent(): void
    {
        $accumulator = new SumAccumulator(ref('value'), false, flow_context());
        $accumulator->accumulate(row(int_entry('value', 10)));

        static::assertSame(10, $accumulator->value());
        static::assertSame(10, $accumulator->value());

        $accumulator->accumulate(row(int_entry('value', 5)));

        static::assertSame(15, $accumulator->value());
    }

    public function test_whole_float_result_stays_float(): void
    {
        $accumulator = new SumAccumulator(ref('value'), false, flow_context());

        $accumulator->accumulate(row(float_entry('value', 1.5)));
        $accumulator->accumulate(row(float_entry('value', 2.5)));

        static::assertSame(4.0, $accumulator->value());
    }
}
