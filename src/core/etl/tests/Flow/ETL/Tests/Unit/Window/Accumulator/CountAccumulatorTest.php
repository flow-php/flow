<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Window\Accumulator;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Window\Accumulator\CountAccumulator;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class CountAccumulatorTest extends FlowTestCase
{
    public function test_counts_every_row_when_reference_is_null(): void
    {
        $accumulator = new CountAccumulator(null, flow_context());

        $accumulator->accumulate(array_to_rows([['value' => 1]], schema(int_schema('value'))), 0);
        $accumulator->accumulate(array_to_rows([['other' => 'x']], schema(str_schema('other'))), 0);

        static::assertSame(2, $accumulator->value());
    }

    public function test_counts_only_non_null_values(): void
    {
        $accumulator = new CountAccumulator(ref('value'), flow_context());

        $accumulator->accumulate(array_to_rows([['value' => 1]], schema(int_schema('value'))), 0);
        $accumulator->accumulate(array_to_rows([['value' => null]], schema(int_schema('value', nullable: true))), 0);
        $accumulator->accumulate(array_to_rows([['value' => 3]], schema(int_schema('value'))), 0);

        static::assertSame(2, $accumulator->value());
    }

    public function test_empty_frame_counts_zero(): void
    {
        static::assertSame(0, (new CountAccumulator(ref('value'), flow_context()))->value());
    }

    public function test_empty_frame_counts_zero_without_reference(): void
    {
        static::assertSame(0, (new CountAccumulator(null, flow_context()))->value());
    }

    public function test_missing_entry_throws_in_strict_mode(): void
    {
        $context = flow_context();
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/^Count window function error: /');

        (new CountAccumulator(ref('value'), $context))->accumulate(array_to_rows([[
            'other' => 'x',
        ]], schema(str_schema('other'))), 0);
    }

    public function test_value_is_idempotent(): void
    {
        $accumulator = new CountAccumulator(ref('value'), flow_context());
        $accumulator->accumulate(array_to_rows([['value' => 1]], schema(int_schema('value'))), 0);

        static::assertSame(1, $accumulator->value());
        static::assertSame(1, $accumulator->value());

        $accumulator->accumulate(array_to_rows([['value' => 2]], schema(int_schema('value'))), 0);

        static::assertSame(2, $accumulator->value());
    }
}
