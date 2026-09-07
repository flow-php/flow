<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\Function\WindowFunction;
use Flow\ETL\Processor\WindowProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\WindowProcessorContext;
use Flow\ETL\Tests\Double\CountingFrameAccumulating;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\current_row;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\following;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\preceding;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\row_number;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\window;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_optional;

final class WindowProcessorTest extends FlowTestCase
{
    public function test_bind_adds_the_window_function_column(): void
    {
        static::assertEquals(
            schema(str_schema('group'), int_schema('value'), int_schema('rn')),
            (new WindowProcessor('rn', row_number()->over(window()->partitionBy(ref('group')))))->bind(schema(
                str_schema('group'),
                int_schema('value'),
            ))->output,
        );
    }

    public function test_bind_refuses_a_partition_column_missing_from_the_input(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        (new WindowProcessor('rn', row_number()->over(window()->partitionBy(ref('missing')))))->bind(schema(int_schema(
            'value',
        )));
    }

    public function test_constant_frame_accumulates_once_per_partition(): void
    {
        $function = new CountingFrameAccumulating(ref('value'));

        static::assertSame(
            [100.0, 100.0, 100.0, 100.0],
            WindowProcessorContext::values(
                'total',
                $function->over(window()->partitionBy(ref('group'))),
                rows(
                    schema(str_schema('group'), int_schema('value')),
                    row(['group' => 'a', 'value' => 10]),
                    row(['group' => 'a', 'value' => 20]),
                    row(['group' => 'a', 'value' => 30]),
                    row(['group' => 'a', 'value' => 40]),
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
            [10.0, 30.0, 60.0, 100.0],
            WindowProcessorContext::values(
                'total',
                $function->over(window()->orderBy(ref('value'))),
                rows(
                    schema(int_schema('value')),
                    row(['value' => 10]),
                    row(['value' => 20]),
                    row(['value' => 30]),
                    row(['value' => 40]),
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
            [20.0, 20.0, 40.0, 70.0],
            WindowProcessorContext::values(
                'total',
                $function->over(window()->orderBy(ref('value'))),
                rows(
                    schema(int_schema('value')),
                    row(['value' => 10]),
                    row(['value' => 10]),
                    row(['value' => 20]),
                    row(['value' => 30]),
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
            [10.0, 30.0, 50.0, 70.0],
            WindowProcessorContext::values(
                'total',
                $function->over(window()->orderBy(ref('value'))->rowsBetween(preceding(1), current_row())),
                rows(
                    schema(int_schema('value')),
                    row(['value' => 10]),
                    row(['value' => 20]),
                    row(['value' => 30]),
                    row(['value' => 40]),
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
                schema(str_schema('group'), int_schema('value')),
                row(['group' => 'a', 'value' => 40]),
                row(['group' => 'a', 'value' => 60]),
            ),
        );

        static::assertCount(1, $batches);
        static::assertEquals(int_schema('total')->addMetadata('origin', 'window'), $batches[0]->schema()->get('total'));

        foreach ($batches[0] as $row) {
            static::assertSame(100, $row->get('total'));
        }
    }

    public function test_an_unknown_partition_reference_is_refused_at_bind(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for entry "missing" not found.');

        WindowProcessorContext::values(
            'row_number',
            row_number()->over(window()->partitionBy(ref('missing'))->orderBy(ref('value'))),
            rows(schema(int_schema('value')), row(['value' => 10]), row(['value' => 20]), row(['value' => 30])),
        );
    }

    public function test_an_unknown_order_reference_is_refused_at_bind(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for entry "missing" not found.');

        WindowProcessorContext::values(
            'row_number',
            row_number()->over(window()->orderBy(ref('missing'))),
            rows(schema(int_schema('value')), row(['value' => 10]), row(['value' => 20]), row(['value' => 30])),
        );
    }

    public function test_empty_frame_yields_the_accumulator_empty_value(): void
    {
        static::assertSame(
            [null, null, null],
            WindowProcessorContext::values(
                'total',
                sum(ref('value'))->over(window()->orderBy(ref('value'))->rowsBetween(following(10), following(20))),
                rows(schema(int_schema('value')), row(['value' => 10]), row(['value' => 20]), row(['value' => 30])),
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
            schema(str_schema('group'), int_schema('value')),
            row(['group' => 'a', 'value' => 10]),
            row(['group' => 'a', 'value' => 20]),
            row(['group' => 'a', 'value' => 30]),
            row(['group' => 'a', 'value' => 40]),
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
            [100.0, 100.0, 100.0, 100.0],
            WindowProcessorContext::values(
                'total',
                sum(ref('value'))->over(window()->partitionBy(ref('group'))),
                rows(
                    schema(str_schema('group'), int_schema('value')),
                    row(['group' => 'a', 'value' => 10]),
                    row(['group' => 'a', 'value' => 20]),
                    row(['group' => 'a', 'value' => 30]),
                    row(['group' => 'a', 'value' => 40]),
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
                schema(str_schema('category'), int_schema('amount')),
                row(['category' => 'a', 'amount' => 100]),
                row(['category' => 'a', 'amount' => 200]),
                row(['category' => 'b', 'amount' => 150]),
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
            yield rows(
                schema(int_schema('amount')),
                row(['amount' => 300]),
                row(['amount' => 100]),
                row(['amount' => 200]),
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

        static::assertCount(3, $allRows);
        static::assertContainsOnlyInt(array_column($allRows, 'rank'));
    }

    /**
     * @param callable(): (AggregatingFunction&WindowFunction) $factory
     */
    #[DataProvider('window_capable_aggregates')]
    public function test_a_windowed_aggregate_has_the_same_type_as_the_plain_aggregate(callable $factory): void
    {
        static::assertEquals($factory()->returns(), $factory()->over(window()->partitionBy(ref('group')))->returns());
    }

    /**
     * @return Generator<string, array{callable(): (AggregatingFunction&WindowFunction)}>
     */
    public static function window_capable_aggregates(): Generator
    {
        yield 'sum' => [static fn() => sum(ref('value'))];
        yield 'count' => [static fn() => count(ref('value'))];
        yield 'average' => [static fn() => average(ref('value'))];
    }

    public function test_the_frame_does_not_change_the_declared_type(): void
    {
        $windows = [
            window()->partitionBy(ref('group')),
            window()->orderBy(ref('value')),
            window()->orderBy(ref('value'))->rowsBetween(preceding(1), current_row()),
            window()->orderBy(ref('value'))->rowsBetween(following(10), following(20)),
        ];

        foreach ($windows as $window) {
            static::assertEquals(type_optional(type_float()), sum(ref('value'))->over($window)->returns());
        }
    }

    /**
     * The window invariant: a running sum over [1, 2.5, 3] used to produce IntegerEntry, FloatEntry,
     * FloatEntry in one Rows - the column now carries one Definition for the whole run. The input
     * column is declared float because a batch can no longer hold two numeric types under one name.
     */
    public function test_a_running_sum_over_mixed_numerics_yields_one_definition(): void
    {
        $batches = WindowProcessorContext::batches(
            'total',
            sum(ref('value'))->over(window()->orderBy(ref('value'))),
            rows(schema(float_schema('value')), row(['value' => 1.0]), row(['value' => 2.5]), row(['value' => 3.0])),
        );

        $definitions = [];
        $values = [];

        foreach ($batches as $batch) {
            $definitions[] = $batch->schema()->get('total');

            foreach ($batch as $row) {
                $values[] = $row->get('total');
            }
        }

        static::assertCount(3, $values);
        static::assertEquals([float_schema('total', true)], array_unique($definitions, SORT_REGULAR));
    }

    public function test_processes_multiple_partitions(): void
    {
        $windowFunction = rank()->over(window()->partitionBy(ref('group'))->orderBy(ref('value')));

        $processor = new WindowProcessor('rank', $windowFunction);

        $generator = (static function () {
            yield rows(
                schema(str_schema('group'), int_schema('value')),
                row(['group' => 'a', 'value' => 10]),
                row(['group' => 'a', 'value' => 20]),
                row(['group' => 'b', 'value' => 5]),
                row(['group' => 'b', 'value' => 15]),
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

    public function test_a_leading_empty_batch_is_yielded_under_the_bound_schema(): void
    {
        $processor = new WindowProcessor('row_number', row_number()->over(window()->orderBy(ref('value'))));

        $generator = (static function () {
            yield rows(schema(int_schema('value')));
            yield rows(schema(int_schema('value')), row(['value' => 10]), row(['value' => 20]));
        })();

        /** @var list<Rows> $batches */
        $batches = iterator_to_array($processor->process($generator, flow_context()), preserve_keys: false);
        $values = [];

        foreach ($batches[1] as $row) {
            $values[] = $row->get('row_number');
        }

        static::assertCount(2, $batches);
        static::assertCount(0, $batches[0]);
        static::assertSame(['value', 'row_number'], $batches[0]->schema()->references()->names());
        static::assertSame([1, 2], $values);
    }
}
