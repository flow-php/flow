<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Processor;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Functions\WindowFunction;
use Flow\ETL\Operators\Processor\WindowProcessor;
use Flow\ETL\Tests\Context\WindowProcessorContext;
use Flow\ETL\Tests\Double\CountingFrameAccumulating;
use Flow\ETL\Tests\Double\WindowFunctionOverOperand;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ListColumnsMother;
use Flow\ETL\Values\Rows;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\current_row;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\following;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\preceding;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
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
    public function test_bind_replaces_an_input_column_named_like_the_window_column(): void
    {
        static::assertEquals(
            schema(int_schema('n'), int_schema('rank')),
            (new WindowProcessor('rank', rank()->over(window()->orderBy(ref('n')))))->bind(schema(
                int_schema('n'),
                str_schema('rank'),
            ))->output,
        );
    }

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
                array_to_rows(
                    [
                        ['group' => 'a', 'value' => 10],
                        ['group' => 'a', 'value' => 20],
                        ['group' => 'a', 'value' => 30],
                        ['group' => 'a', 'value' => 40],
                    ],
                    schema(str_schema('group'), int_schema('value')),
                ),
            ),
        );
        static::assertSame(1, $function->accumulatorCalls);
        static::assertSame(4, $function->fedRows);
    }

    /**
     * A peer frame grows by one row per row, so the accumulator carries forward and the whole partition
     * feeds n rows instead of n(n+1)/2.
     */
    public function test_growing_frame_accumulates_each_row_once(): void
    {
        $function = new CountingFrameAccumulating(ref('value'));

        static::assertSame(
            [10.0, 30.0, 60.0, 100.0],
            WindowProcessorContext::values(
                'total',
                $function->over(window()->orderBy(ref('value'))),
                array_to_rows([
                    ['value' => 10],
                    ['value' => 20],
                    ['value' => 30],
                    ['value' => 40],
                ], schema(int_schema('value'))),
            ),
        );
        static::assertSame(1, $function->accumulatorCalls);
        static::assertSame(4, $function->fedRows);
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
                array_to_rows([
                    ['value' => 10],
                    ['value' => 10],
                    ['value' => 20],
                    ['value' => 30],
                ], schema(int_schema('value'))),
            ),
        );
        static::assertSame(1, $function->accumulatorCalls);
        static::assertSame(4, $function->fedRows);
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
                array_to_rows([
                    ['value' => 10],
                    ['value' => 20],
                    ['value' => 30],
                    ['value' => 40],
                ], schema(int_schema('value'))),
            ),
        );
        static::assertSame(3, $function->accumulatorCalls);
        static::assertSame(6, $function->fedRows);
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
            array_to_rows(
                [['group' => 'a', 'value' => 40], ['group' => 'a', 'value' => 60]],
                schema(str_schema('group'), int_schema('value')),
            ),
        );

        static::assertCount(1, $batches);
        static::assertEquals(int_schema('total')->addMetadata('origin', 'window'), $batches[0]->schema()->get('total'));

        foreach ($batches[0]->toArray() as $row) {
            static::assertSame(100, $row['total']);
        }
    }

    public function test_an_unknown_partition_reference_is_refused_at_bind(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for column "missing" not found.');

        WindowProcessorContext::values(
            'row_number',
            row_number()->over(window()->partitionBy(ref('missing'))->orderBy(ref('value'))),
            array_to_rows([['value' => 10], ['value' => 20], ['value' => 30]], schema(int_schema('value'))),
        );
    }

    public function test_an_unknown_order_reference_is_refused_at_bind(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for column "missing" not found.');

        WindowProcessorContext::values(
            'row_number',
            row_number()->over(window()->orderBy(ref('missing'))),
            array_to_rows([['value' => 10], ['value' => 20], ['value' => 30]], schema(int_schema('value'))),
        );
    }

    public function test_empty_frame_yields_the_accumulator_empty_value(): void
    {
        static::assertSame(
            [null, null, null],
            WindowProcessorContext::values(
                'total',
                sum(ref('value'))->over(window()->orderBy(ref('value'))->rowsBetween(following(10), following(20))),
                array_to_rows([['value' => 10], ['value' => 20], ['value' => 30]], schema(int_schema('value'))),
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
        $partition = array_to_rows(
            [
                ['group' => 'a', 'value' => 10],
                ['group' => 'a', 'value' => 20],
                ['group' => 'a', 'value' => 30],
                ['group' => 'a', 'value' => 40],
            ],
            schema(str_schema('group'), int_schema('value')),
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
                array_to_rows(
                    [
                        ['group' => 'a', 'value' => 10],
                        ['group' => 'a', 'value' => 20],
                        ['group' => 'a', 'value' => 30],
                        ['group' => 'a', 'value' => 40],
                    ],
                    schema(str_schema('group'), int_schema('value')),
                ),
            ),
        );
    }

    public function test_applies_window_function(): void
    {
        /** @var list<Rows> $result */
        $result = iterator_to_array((new WindowProcessor(
            'rank',
            rank()->over(window()->partitionBy(ref('category'))->orderBy(ref('amount')->desc())),
        ))->process(
            (static function () {
                yield array_to_rows(
                    [
                        ['category' => 'a', 'amount' => 100],
                        ['category' => 'a', 'amount' => 200],
                        ['category' => 'b', 'amount' => 150],
                    ],
                    schema(str_schema('category'), int_schema('amount')),
                );
            })(),
            flow_context()->backend(),
        ));
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
        static::assertCount(
            0,
            iterator_to_array((new WindowProcessor('rank', rank()->over(window()->orderBy(ref('amount')))))->process(
                (static function () {
                    yield from [];
                })(),
                flow_context()->backend(),
            )),
        );
    }

    public function test_handles_single_partition(): void
    {
        /** @var list<Rows> $result */
        $result = iterator_to_array((new WindowProcessor(
            'rank',
            rank()->over(window()->orderBy(ref('amount')->desc())),
        ))->process(
            (static function () {
                yield array_to_rows([
                    ['amount' => 300],
                    ['amount' => 100],
                    ['amount' => 200],
                ], schema(int_schema('amount')));
            })(),
            flow_context()->backend(),
        ));
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
            array_to_rows([['value' => 1.0], ['value' => 2.5], ['value' => 3.0]], schema(float_schema('value'))),
        );

        $definitions = [];
        $values = [];

        foreach ($batches as $batch) {
            $definitions[] = $batch->schema()->get('total');

            foreach ($batch->toArray() as $row) {
                $values[] = $row['total'];
            }
        }

        static::assertCount(3, $values);
        static::assertEquals([float_schema('total', true)], array_unique($definitions, SORT_REGULAR));
    }

    public function test_processes_multiple_partitions(): void
    {
        /** @var list<Rows> $result */
        $result = iterator_to_array((new WindowProcessor(
            'rank',
            rank()->over(window()->partitionBy(ref('group'))->orderBy(ref('value'))),
        ))->process(
            (static function () {
                yield array_to_rows(
                    [
                        ['group' => 'a', 'value' => 10],
                        ['group' => 'a', 'value' => 20],
                        ['group' => 'b', 'value' => 5],
                        ['group' => 'b', 'value' => 15],
                    ],
                    schema(str_schema('group'), int_schema('value')),
                );
            })(),
            flow_context()->backend(),
        ));
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
        $batches = iterator_to_array(
            (new WindowProcessor('row_number', row_number()->over(window()->orderBy(ref('value')))))->process(
                (static function () {
                    yield rows(schema(int_schema('value')));
                    yield array_to_rows([['value' => 10], ['value' => 20]], schema(int_schema('value')));
                })(),
                flow_context()->backend(),
            ),
            preserve_keys: false,
        );
        $values = [];

        foreach ($batches[1]->toArray() as $row) {
            $values[] = $row['row_number'];
        }

        static::assertCount(2, $batches);
        static::assertCount(0, $batches[0]);
        static::assertSame(['value', 'row_number'], $batches[0]->schema()->names());
        static::assertSame([1, 2], $values);
    }

    public function test_each_input_batch_is_a_partition_of_its_own(): void
    {
        $schema = schema(str_schema('dept'), int_schema('salary'));
        $result = iterator_to_array(
            (new WindowProcessor(
                'row_num',
                row_number()->over(window()->partitionBy(ref('dept'))->orderBy(ref('salary'))),
            ))->process(
                (static function () use ($schema) {
                    yield array_to_rows([
                        ['dept' => 'IT', 'salary' => 5000],
                        ['dept' => 'IT', 'salary' => 6000],
                    ], $schema);
                    yield array_to_rows([['dept' => 'HR', 'salary' => 4000]], $schema);
                })(),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertCount(2, $result);
        static::assertSame([1, 2], [$result[0]->column('row_num')->value(0), $result[0]->column('row_num')->value(1)]);
        static::assertSame([1], [$result[1]->column('row_num')->value(0)]);
    }

    public function test_row_number_counts_the_rows_in_order_without_a_partition(): void
    {
        $result = iterator_to_array(
            (new WindowProcessor('row_num', row_number()->over(window()->orderBy(ref('id')))))->process(
                (static function () {
                    yield array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));
                })(),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertCount(1, $result);
        static::assertSame([1, 2, 3], [
            $result[0]->column('row_num')->value(0),
            $result[0]->column('row_num')->value(1),
            $result[0]->column('row_num')->value(2),
        ]);
    }

    public function test_rows_come_out_sorted_by_the_order_by(): void
    {
        $result = iterator_to_array(
            (new WindowProcessor(
                'row_num',
                row_number()->over(window()->partitionBy(ref('dept'))->orderBy(ref('salary'))),
            ))->process(
                (static function () {
                    yield array_to_rows(
                        [
                            ['dept' => 'IT', 'salary' => 6000],
                            ['dept' => 'IT', 'salary' => 5000],
                            ['dept' => 'IT', 'salary' => 7000],
                        ],
                        schema(str_schema('dept'), int_schema('salary')),
                    );
                })(),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertSame(
            [
                ['dept' => 'IT', 'salary' => 5000, 'row_num' => 1],
                ['dept' => 'IT', 'salary' => 6000, 'row_num' => 2],
                [
                    'dept' => 'IT',
                    'salary' => 7000,
                    'row_num' => 3,
                ],
            ],
            $result[0]->toArray(),
        );
    }

    public function test_bind_refuses_an_expand_in_the_window_function(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'over() cannot contain array_expand(), it turns one row into many rows. Expand with withColumn() first, then use the new column.',
        );

        (new WindowProcessor(
            's',
            (new WindowFunctionOverOperand(ref('flags')->expand()->equals(lit(true))))->over(window()->partitionBy(ref(
                'n',
            ))),
        ))->bind(ListColumnsMother::numberAndFlagsSchema());
    }
}
