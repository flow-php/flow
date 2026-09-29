<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Window\Accumulator;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Window\Accumulator\SumAccumulator;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class SumAccumulatorTest extends FlowTestCase
{
    public function test_empty_frame_sums_to_null(): void
    {
        static::assertNull((new SumAccumulator(ref('value'), false, flow_context()))->value());
    }

    public function test_exact_sum_avoids_floating_point_drift(): void
    {
        $accumulator = new SumAccumulator(ref('value'), true, flow_context());

        $accumulator->accumulate(array_to_rows([['value' => 0.1]], schema(float_schema('value'))), 0);
        $accumulator->accumulate(array_to_rows([['value' => 0.2]], schema(float_schema('value'))), 0);

        static::assertSame(0.3, $accumulator->value());
    }

    public function test_inexact_sum_keeps_native_addition(): void
    {
        $accumulator = new SumAccumulator(ref('value'), false, flow_context());

        $accumulator->accumulate(array_to_rows([['value' => 0.1]], schema(float_schema('value'))), 0);
        $accumulator->accumulate(array_to_rows([['value' => 0.2]], schema(float_schema('value'))), 0);

        static::assertSame(0.1 + 0.2, $accumulator->value());
    }

    public function test_missing_entry_throws_in_strict_mode(): void
    {
        $context = flow_context();
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/^Sum window function error: /');

        (new SumAccumulator(ref('value'), false, $context))->accumulate(array_to_rows([[
            'other' => 'x',
        ]], schema(str_schema('other'))), 0);
    }

    public function test_non_numeric_values_are_skipped(): void
    {
        $accumulator = new SumAccumulator(ref('value'), false, flow_context());

        $accumulator->accumulate(array_to_rows([['value' => 10]], schema(int_schema('value'))), 0);
        $accumulator->accumulate(array_to_rows([['value' => 'not a number']], schema(str_schema('value'))), 0);
        $accumulator->accumulate(array_to_rows([['value' => null]], schema(str_schema('value', nullable: true))), 0);
        $accumulator->accumulate(array_to_rows([['value' => 5]], schema(int_schema('value'))), 0);

        static::assertSame(15, $accumulator->value());
    }

    public function test_numeric_strings_are_summed(): void
    {
        $accumulator = new SumAccumulator(ref('value'), false, flow_context());

        $accumulator->accumulate(array_to_rows([['value' => '10']], schema(str_schema('value'))), 0);
        $accumulator->accumulate(array_to_rows([['value' => '2.5']], schema(str_schema('value'))), 0);

        static::assertSame(12.5, $accumulator->value());
    }

    public function test_only_non_numeric_values_still_sums_to_null(): void
    {
        $accumulator = new SumAccumulator(ref('value'), false, flow_context());

        $accumulator->accumulate(array_to_rows([['value' => null]], schema(str_schema('value', nullable: true))), 0);

        static::assertNull($accumulator->value());
    }

    public function test_value_is_idempotent(): void
    {
        $accumulator = new SumAccumulator(ref('value'), false, flow_context());
        $accumulator->accumulate(array_to_rows([['value' => 10]], schema(int_schema('value'))), 0);

        static::assertSame(10, $accumulator->value());
        static::assertSame(10, $accumulator->value());

        $accumulator->accumulate(array_to_rows([['value' => 5]], schema(int_schema('value'))), 0);

        static::assertSame(15, $accumulator->value());
    }

    public function test_whole_float_result_stays_float(): void
    {
        $accumulator = new SumAccumulator(ref('value'), false, flow_context());

        $accumulator->accumulate(array_to_rows([['value' => 1.5]], schema(float_schema('value'))), 0);
        $accumulator->accumulate(array_to_rows([['value' => 2.5]], schema(float_schema('value'))), 0);

        static::assertSame(4.0, $accumulator->value());
    }
}
