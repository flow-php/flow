<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Processor\WindowProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\WindowProcessorContext;
use Flow\ETL\Tests\Double\CountingFrameAccumulating;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\current_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\following;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\preceding;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\row_number;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\window;

final class WindowProcessorTest extends FlowTestCase
{
    public function test_constant_frame_accumulates_once_per_partition(): void
    {
        $function = new CountingFrameAccumulating(ref('value'));

        static::assertSame(
            [100, 100, 100, 100],
            WindowProcessorContext::values(
                'total',
                $function->over(window()->partitionBy(ref('group'))),
                rows(
                    row(str_entry('group', 'a'), int_entry('value', 10)),
                    row(str_entry('group', 'a'), int_entry('value', 20)),
                    row(str_entry('group', 'a'), int_entry('value', 30)),
                    row(str_entry('group', 'a'), int_entry('value', 40)),
                ),
            ),
        );
        static::assertSame(1, $function->accumulatorCalls);
        static::assertSame(4, $function->accumulateCalls);
    }

    /**
     * A peer frame grows by one row per row, so the accumulator carries forward and the whole partition
     * costs n accumulate() calls instead of n(n+1)/2.
     */
    public function test_growing_frame_accumulates_each_row_once(): void
    {
        $function = new CountingFrameAccumulating(ref('value'));

        static::assertSame(
            [10, 30, 60, 100],
            WindowProcessorContext::values(
                'total',
                $function->over(window()->orderBy(ref('value'))),
                rows(
                    row(int_entry('value', 10)),
                    row(int_entry('value', 20)),
                    row(int_entry('value', 30)),
                    row(int_entry('value', 40)),
                ),
            ),
        );
        static::assertSame(1, $function->accumulatorCalls);
        static::assertSame(4, $function->accumulateCalls);
    }

    /**
     * Rows tied on the ordering column share frame bounds, so they share one computed value.
     */
    public function test_peer_group_reuses_the_value_within_a_group(): void
    {
        $function = new CountingFrameAccumulating(ref('value'));

        static::assertSame(
            [20, 20, 40, 70],
            WindowProcessorContext::values(
                'total',
                $function->over(window()->orderBy(ref('value'))),
                rows(
                    row(int_entry('value', 10)),
                    row(int_entry('value', 10)),
                    row(int_entry('value', 20)),
                    row(int_entry('value', 30)),
                ),
            ),
        );
        static::assertSame(1, $function->accumulatorCalls);
        static::assertSame(4, $function->accumulateCalls);
    }

    /**
     * A sliding frame moves its start, so the accumulator cannot be carried and must be rebuilt.
     */
    public function test_sliding_frame_recomputes_per_row(): void
    {
        $function = new CountingFrameAccumulating(ref('value'));

        static::assertSame(
            [10, 30, 50, 70],
            WindowProcessorContext::values(
                'total',
                $function->over(window()->orderBy(ref('value'))->rowsBetween(preceding(1), current_row())),
                rows(
                    row(int_entry('value', 10)),
                    row(int_entry('value', 20)),
                    row(int_entry('value', 30)),
                    row(int_entry('value', 40)),
                ),
            ),
        );
        static::assertSame(3, $function->accumulatorCalls);
        static::assertSame(6, $function->accumulateCalls);
    }

    /**
     * A Definition entry carries its own type and metadata into the created entry, unlike a plain
     * string name which leaves the type to inference.
     */
    public function test_definition_entry_keeps_its_type_and_metadata(): void
    {
        $batches = WindowProcessorContext::batches(
            int_schema('total')->addMetadata('origin', 'window'),
            sum(ref('value'))->over(window()->partitionBy(ref('group'))),
            rows(
                row(str_entry('group', 'a'), int_entry('value', 40)),
                row(str_entry('group', 'a'), int_entry('value', 60)),
            ),
        );

        static::assertCount(1, $batches);

        foreach ($batches[0] as $row) {
            static::assertSame(100, $row->valueOf('total'));
            static::assertSame('window', $row->get('total')->definition()->metadata()->get('origin'));
        }
    }

    public function test_rows_missing_the_partition_key_fall_into_one_partition(): void
    {
        static::assertSame(
            [1, 2, 3],
            WindowProcessorContext::values(
                'row_number',
                row_number()->over(window()->partitionBy(ref('missing'))->orderBy(ref('value'))),
                rows(row(int_entry('value', 10)), row(int_entry('value', 20)), row(int_entry('value', 30))),
            ),
        );
    }

    public function test_empty_frame_yields_the_accumulator_empty_value(): void
    {
        static::assertSame(
            [null, null, null],
            WindowProcessorContext::values(
                'total',
                sum(ref('value'))->over(window()->orderBy(ref('value'))->rowsBetween(following(10), following(20))),
                rows(row(int_entry('value', 10)), row(int_entry('value', 20)), row(int_entry('value', 30))),
            ),
        );
    }

    /**
     * row_number() ignores the frame entirely, so it must not be memoised even though an unordered
     * window produces identical bounds for every row. Memoising on frame constancy alone - without a
     * signal from the function - writes 1 into every row of the partition.
     */
    public function test_row_number_is_not_memoised_by_a_constant_frame(): void
    {
        $partition = rows(
            row(str_entry('group', 'a'), int_entry('value', 10)),
            row(str_entry('group', 'a'), int_entry('value', 20)),
            row(str_entry('group', 'a'), int_entry('value', 30)),
            row(str_entry('group', 'a'), int_entry('value', 40)),
        );
        $frame = window()->partitionBy(ref('group'))->frame();

        static::assertSame($frame->bounds(0, $partition), $frame->bounds(3, $partition));
        static::assertSame(
            [1, 2, 3, 4],
            WindowProcessorContext::values(
                'row_number',
                row_number()->over(window()->partitionBy(ref('group'))),
                $partition,
            ),
        );
    }

    public function test_whole_partition_frame_gives_every_row_the_partition_total(): void
    {
        static::assertSame(
            [100, 100, 100, 100],
            WindowProcessorContext::values(
                'total',
                sum(ref('value'))->over(window()->partitionBy(ref('group'))),
                rows(
                    row(str_entry('group', 'a'), int_entry('value', 10)),
                    row(str_entry('group', 'a'), int_entry('value', 20)),
                    row(str_entry('group', 'a'), int_entry('value', 30)),
                    row(str_entry('group', 'a'), int_entry('value', 40)),
                ),
            ),
        );
    }

    public function test_applies_window_function(): void
    {
        $windowFunction = rank()->over(window()->partitionBy(ref('category'))->orderBy(ref('amount')->desc()));

        $processor = new WindowProcessor('rank', $windowFunction);

        $generator = (static function () {
            yield rows(
                row(str_entry('category', 'a'), int_entry('amount', 100)),
                row(str_entry('category', 'a'), int_entry('amount', 200)),
                row(str_entry('category', 'b'), int_entry('amount', 150)),
            );
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));
        /** @var list<array<array-key, mixed>> $allRows */
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertCount(3, $allRows);
        static::assertArrayHasKey('rank', $allRows[0]);
    }

    public function test_handles_empty_input(): void
    {
        $windowFunction = rank()->over(window()->orderBy(ref('amount')));

        $processor = new WindowProcessor('rank', $windowFunction);

        $generator = (static function () {
            yield from [];
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        static::assertCount(0, $result);
    }

    public function test_handles_single_partition(): void
    {
        $windowFunction = rank()->over(window()->orderBy(ref('amount')->desc()));

        $processor = new WindowProcessor('rank', $windowFunction);

        $generator = (static function () {
            yield rows(row(int_entry('amount', 300)), row(int_entry('amount', 100)), row(int_entry('amount', 200)));
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertCount(3, $allRows);
        static::assertContainsOnlyInt(array_column($allRows, 'rank'));
    }

    public function test_processes_multiple_partitions(): void
    {
        $windowFunction = rank()->over(window()->partitionBy(ref('group'))->orderBy(ref('value')));

        $processor = new WindowProcessor('rank', $windowFunction);

        $generator = (static function () {
            yield rows(
                row(str_entry('group', 'a'), int_entry('value', 10)),
                row(str_entry('group', 'a'), int_entry('value', 20)),
                row(str_entry('group', 'b'), int_entry('value', 5)),
                row(str_entry('group', 'b'), int_entry('value', 15)),
            );
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertCount(4, $allRows);

        $groupA = array_filter($allRows, static fn(array $r): bool => $r['group'] === 'a');
        $groupB = array_filter($allRows, static fn(array $r): bool => $r['group'] === 'b');

        static::assertCount(2, $groupA);
        static::assertCount(2, $groupB);
    }
}
