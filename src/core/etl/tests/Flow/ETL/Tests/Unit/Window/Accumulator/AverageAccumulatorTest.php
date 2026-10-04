<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Window\Accumulator;

use Flow\Calculator\Rounding;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Window\Accumulator\AverageAccumulator;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class AverageAccumulatorTest extends FlowTestCase
{
    public function test_averages_accumulated_values(): void
    {
        $accumulator = new AverageAccumulator(ref('value'), 2, Rounding::HALF_UP, false, flow_context());

        $accumulator->accumulate(array_to_rows([['value' => 10]], schema(int_schema('value'))), 0);
        $accumulator->accumulate(array_to_rows([['value' => 20]], schema(int_schema('value'))), 0);

        static::assertSame(15.0, $accumulator->value());
    }

    public function test_empty_frame_averages_to_null(): void
    {
        static::assertNull(
            (new AverageAccumulator(ref('value'), 2, Rounding::HALF_UP, false, flow_context()))->value(),
        );
    }

    public function test_missing_entry_throws_in_strict_mode(): void
    {
        $context = flow_context();
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/^Average window function error: /');

        (new AverageAccumulator(ref('value'), 2, Rounding::HALF_UP, false, $context))->accumulate(array_to_rows([[
            'other' => 'x',
        ]], schema(str_schema('other'))), 0);
    }

    public function test_non_numeric_values_are_skipped(): void
    {
        $accumulator = new AverageAccumulator(ref('value'), 2, Rounding::HALF_UP, false, flow_context());

        $accumulator->accumulate(array_to_rows([['value' => 10]], schema(int_schema('value'))), 0);
        $accumulator->accumulate(array_to_rows([['value' => 'not a number']], schema(str_schema('value'))), 0);
        $accumulator->accumulate(array_to_rows([['value' => null]], schema(str_schema('value', nullable: true))), 0);
        $accumulator->accumulate(array_to_rows([['value' => 20]], schema(int_schema('value'))), 0);

        static::assertSame(15.0, $accumulator->value());
    }

    public function test_only_null_values_still_average_to_null(): void
    {
        $accumulator = new AverageAccumulator(ref('value'), 2, Rounding::HALF_UP, false, flow_context());

        $accumulator->accumulate(array_to_rows([['value' => null]], schema(str_schema('value', nullable: true))), 0);

        static::assertNull($accumulator->value());
    }

    public function test_scale_and_rounding_are_applied(): void
    {
        $accumulator = new AverageAccumulator(ref('value'), 3, Rounding::HALF_UP, false, flow_context());

        $accumulator->accumulate(array_to_rows([['value' => 10]], schema(int_schema('value'))), 0);
        $accumulator->accumulate(array_to_rows([['value' => 20]], schema(int_schema('value'))), 0);
        $accumulator->accumulate(array_to_rows([['value' => 25]], schema(int_schema('value'))), 0);

        static::assertSame(18.333, $accumulator->value());
    }

    public function test_value_is_idempotent(): void
    {
        $accumulator = new AverageAccumulator(ref('value'), 2, Rounding::HALF_UP, false, flow_context());
        $accumulator->accumulate(array_to_rows([['value' => 10]], schema(int_schema('value'))), 0);

        static::assertSame(10.0, $accumulator->value());
        static::assertSame(10.0, $accumulator->value());

        $accumulator->accumulate(array_to_rows([['value' => 20]], schema(int_schema('value'))), 0);

        static::assertSame(15.0, $accumulator->value());
    }
}
