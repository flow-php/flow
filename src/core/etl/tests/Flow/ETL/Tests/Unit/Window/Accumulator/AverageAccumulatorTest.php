<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Window\Accumulator;

use Flow\Calculator\Rounding;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Window\Accumulator\AverageAccumulator;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class AverageAccumulatorTest extends FlowTestCase
{
    public function test_averages_accumulated_values(): void
    {
        $accumulator = new AverageAccumulator(ref('value'), 2, Rounding::HALF_UP, flow_context());

        $accumulator->accumulate(row(int_entry('value', 10)));
        $accumulator->accumulate(row(int_entry('value', 20)));

        static::assertSame(15.0, $accumulator->value());
    }

    public function test_empty_frame_averages_to_null(): void
    {
        static::assertNull((new AverageAccumulator(ref('value'), 2, Rounding::HALF_UP, flow_context()))->value());
    }

    public function test_missing_entry_throws_in_strict_mode(): void
    {
        $context = flow_context();
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/^Average window function error: /');

        (new AverageAccumulator(ref('value'), 2, Rounding::HALF_UP, $context))->accumulate(row(str_entry(
            'other',
            'x',
        )));
    }

    public function test_non_numeric_values_are_skipped(): void
    {
        $accumulator = new AverageAccumulator(ref('value'), 2, Rounding::HALF_UP, flow_context());

        $accumulator->accumulate(row(int_entry('value', 10)));
        $accumulator->accumulate(row(str_entry('value', 'not a number')));
        $accumulator->accumulate(row(int_entry('value', null)));
        $accumulator->accumulate(row(int_entry('value', 20)));

        static::assertSame(15.0, $accumulator->value());
    }

    public function test_only_null_values_still_average_to_null(): void
    {
        $accumulator = new AverageAccumulator(ref('value'), 2, Rounding::HALF_UP, flow_context());

        $accumulator->accumulate(row(int_entry('value', null)));

        static::assertNull($accumulator->value());
    }

    public function test_scale_and_rounding_are_applied(): void
    {
        $accumulator = new AverageAccumulator(ref('value'), 3, Rounding::HALF_UP, flow_context());

        $accumulator->accumulate(row(int_entry('value', 10)));
        $accumulator->accumulate(row(int_entry('value', 20)));
        $accumulator->accumulate(row(int_entry('value', 25)));

        static::assertSame(18.333, $accumulator->value());
    }

    public function test_value_is_idempotent(): void
    {
        $accumulator = new AverageAccumulator(ref('value'), 2, Rounding::HALF_UP, flow_context());
        $accumulator->accumulate(row(int_entry('value', 10)));

        static::assertSame(10.0, $accumulator->value());
        static::assertSame(10.0, $accumulator->value());

        $accumulator->accumulate(row(int_entry('value', 20)));

        static::assertSame(15.0, $accumulator->value());
    }
}
