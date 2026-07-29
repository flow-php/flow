<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Window\Accumulator;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ExecutionMode;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Window\Accumulator\CountAccumulator;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class CountAccumulatorTest extends FlowTestCase
{
    public function test_counts_every_row_when_reference_is_null(): void
    {
        $accumulator = new CountAccumulator(null, flow_context());

        $accumulator->accumulate(row(int_entry('value', 1)));
        $accumulator->accumulate(row(str_entry('other', 'x')));

        static::assertSame(2, $accumulator->value());
    }

    public function test_counts_only_non_null_values(): void
    {
        $accumulator = new CountAccumulator(ref('value'), flow_context());

        $accumulator->accumulate(row(int_entry('value', 1)));
        $accumulator->accumulate(row(int_entry('value', null)));
        $accumulator->accumulate(row(int_entry('value', 3)));

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

    public function test_missing_entry_is_reported_in_lenient_mode(): void
    {
        $context = flow_context();
        $context->functions()->setMode(ExecutionMode::LENIENT);

        $accumulator = new CountAccumulator(ref('value'), $context);
        $accumulator->accumulate(row(str_entry('other', 'x')));

        static::assertSame(0, $accumulator->value());
    }

    public function test_missing_entry_throws_in_strict_mode(): void
    {
        $context = flow_context();
        $context->functions()->setMode(ExecutionMode::STRICT);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/^Count window function error: /');

        (new CountAccumulator(ref('value'), $context))->accumulate(row(str_entry('other', 'x')));
    }

    public function test_value_is_idempotent(): void
    {
        $accumulator = new CountAccumulator(ref('value'), flow_context());
        $accumulator->accumulate(row(int_entry('value', 1)));

        static::assertSame(1, $accumulator->value());
        static::assertSame(1, $accumulator->value());

        $accumulator->accumulate(row(int_entry('value', 2)));

        static::assertSame(2, $accumulator->value());
    }
}
