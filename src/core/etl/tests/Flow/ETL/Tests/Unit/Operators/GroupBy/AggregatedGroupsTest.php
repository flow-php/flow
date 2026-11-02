<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\GroupBy;

use ArrayObject;
use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\GroupBy\AggregatedGroups;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Operators\GroupBy\GroupByShape;
use Flow\ETL\Operators\GroupBy\PartialAggregate;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\Double\SpyColumn;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Rows;

use function array_combine;
use function array_filter;
use function array_keys;
use function array_map;
use function array_values;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_agg;
use function Flow\ETL\DSL\sum;
use function iterator_to_array;

use const PHP_INT_MAX;

final class AggregatedGroupsTest extends FlowTestCase
{
    public function test_it_accumulates_rows_of_the_same_key_into_one_group(): void
    {
        $groupBy = new GroupBy(refs('country'), new Aggregators(sum(ref('amount'))));

        $input = schema(str_schema('country'), int_schema('amount'));
        $groups = new AggregatedGroups($groupBy, GroupByShape::of($groupBy, $input), flow_context(config())->backend());

        $groups->accumulate(array_to_rows([
            ['country' => 'PL', 'amount' => 10],
            ['country' => 'DE', 'amount' => 20],
        ], $input), flow_context(config())->backend());
        $groups->accumulate(array_to_rows([[
            'country' => 'PL',
            'amount' => 30,
        ]], $input), flow_context(config())->backend());

        /** @var list<Rows> $batches */
        $batches = iterator_to_array($groups->flush(1000, flow_context(config())->backend()));

        static::assertCount(1, $batches);
        static::assertSame(
            [
                ['country' => 'PL', 'amount_sum' => 40.0],
                ['country' => 'DE', 'amount_sum' => 20.0],
            ],
            $batches[0]->toArray(),
        );
    }

    public function test_a_sum_beside_a_string_aggregate_returns_what_each_returns_alone(): void
    {
        $groupBy = new GroupBy(refs('country'), new Aggregators(sum(ref('amount')), string_agg(ref('note'), ',')));
        $input = schema(str_schema('country'), int_schema('amount'), str_schema('note'));
        $groups = new AggregatedGroups($groupBy, GroupByShape::of($groupBy, $input), flow_context(config())->backend());

        $groups->accumulate(array_to_rows([
            ['country' => 'PL', 'amount' => 10, 'note' => 'a'],
            ['country' => 'DE', 'amount' => 20, 'note' => 'b'],
        ], $input), flow_context(config())->backend());
        $groups->accumulate(array_to_rows([
            ['country' => 'PL', 'amount' => 30, 'note' => 'c'],
        ], $input), flow_context(config())->backend());

        static::assertSame(
            [
                ['country' => 'PL', 'amount_sum' => 40.0, 'note_str_agg' => 'a,c'],
                ['country' => 'DE', 'amount_sum' => 20.0, 'note_str_agg' => 'b'],
            ],
            iterator_to_array($groups->flush(1000, flow_context(config())->backend()), false)[0]->toArray(),
        );
    }

    public function test_an_exact_sum_keeps_its_exact_result_across_batches(): void
    {
        $groupBy = new GroupBy(
            refs('country'),
            new Aggregators(sum(ref('amount'), exact: true), sum(ref('amount')->as('fast'))),
        );
        $input = schema(str_schema('country'), float_schema('amount'));
        $groups = new AggregatedGroups($groupBy, GroupByShape::of($groupBy, $input), flow_context(config())->backend());

        $groups->accumulate(array_to_rows([[
            'country' => 'PL',
            'amount' => 0.1,
        ]], $input), flow_context(config())->backend());
        $groups->accumulate(array_to_rows([[
            'country' => 'PL',
            'amount' => 0.2,
        ]], $input), flow_context(config())->backend());

        static::assertSame(
            [['country' => 'PL', 'amount_sum' => 0.3, 'fast' => 0.30000000000000004]],
            iterator_to_array($groups->flush(1000, flow_context(config())->backend()), false)[0]->toArray(),
        );
    }

    public function test_the_flushed_batch_carries_the_shape_output_schema(): void
    {
        $groupBy = new GroupBy(refs('country'), new Aggregators(sum(ref('amount'))));

        $input = schema(str_schema('country'), int_schema('amount'));
        $shape = GroupByShape::of($groupBy, $input);
        $groups = new AggregatedGroups($groupBy, $shape, flow_context(config())->backend());

        $groups->accumulate(array_to_rows([[
            'country' => 'PL',
            'amount' => 10,
        ]], $input), flow_context(config())->backend());

        /** @var list<Rows> $batches */
        $batches = iterator_to_array($groups->flush(1000, flow_context(config())->backend()));

        static::assertEquals($shape->output, $batches[0]->schema());
        static::assertEquals(schema(str_schema('country'), float_schema('amount_sum', nullable: true)), $shape->output);
    }

    public function test_it_splits_the_flushed_groups_into_batches_of_the_requested_size(): void
    {
        $groupBy = new GroupBy(refs('country'), new Aggregators(sum(ref('amount'))));

        $input = schema(str_schema('country'), int_schema('amount'));
        $groups = new AggregatedGroups($groupBy, GroupByShape::of($groupBy, $input), flow_context(config())->backend());

        $groups->accumulate(array_to_rows([
            ['country' => 'PL', 'amount' => 10],
            ['country' => 'DE', 'amount' => 20],
            ['country' => 'FR', 'amount' => 30],
        ], $input), flow_context(config())->backend());

        static::assertSame(
            [2, 1],
            array_map(
                static fn(Rows $batch): int => $batch->count(),
                iterator_to_array($groups->flush(2, flow_context(config())->backend())),
            ),
        );
    }

    public function test_flushing_without_accumulating_yields_nothing(): void
    {
        $groupBy = new GroupBy(refs('country'), new Aggregators(sum(ref('amount'))));

        static::assertSame(
            [],
            iterator_to_array((new AggregatedGroups(
                $groupBy,
                GroupByShape::of($groupBy, schema(str_schema('country'), int_schema('amount'))),
                flow_context(config())->backend(),
            ))->flush(1000, flow_context(config())->backend())),
        );
    }

    public function test_a_row_missing_a_nullable_group_key_lands_in_the_null_group(): void
    {
        $groupBy = new GroupBy(refs('country'), new Aggregators(sum(ref('amount'))));

        $input = schema(str_schema('country', nullable: true), int_schema('amount'));
        $groups = new AggregatedGroups($groupBy, GroupByShape::of($groupBy, $input), flow_context(config())->backend());

        $groups->accumulate(array_to_rows([
            ['amount' => 10],
            ['country' => 'PL', 'amount' => 20],
            ['amount' => 5],
        ], $input), flow_context(config())->backend());

        /** @var list<Rows> $batches */
        $batches = iterator_to_array($groups->flush(1000, flow_context(config())->backend()));

        static::assertSame(
            [
                ['country' => null, 'amount_sum' => 15.0],
                ['country' => 'PL', 'amount_sum' => 20.0],
            ],
            $batches[0]->toArray(),
        );
    }

    public function test_a_sum_and_an_average_of_one_column_read_its_values_once_per_batch(): void
    {
        $calls = new ArrayObject();
        $groupBy = new GroupBy(refs('k'), new Aggregators(sum(ref('v')), average(ref('v'))));
        $input = schema(str_schema('k'), int_schema('v'));
        $rows = array_to_rows([['k' => 'a', 'v' => 1], ['k' => 'b', 'v' => 2], ['k' => 'a', 'v' => 3]], $input);
        $groups = new AggregatedGroups($groupBy, GroupByShape::of($groupBy, $input), flow_context(config())->backend());

        $groups->accumulate(
            Rows::fromColumns($input, ['k' => $rows->column('k'), 'v' => new SpyColumn($rows->column('v'), $calls)], 3),
            flow_context(config())->backend(),
        );

        static::assertSame(['values'], $calls->getArrayCopy());
        static::assertSame(
            [['k' => 'a', 'v_sum' => 4.0, 'v_avg' => 2.0], ['k' => 'b', 'v_sum' => 2.0, 'v_avg' => 2.0]],
            iterator_to_array($groups->flush(1000, flow_context(config())->backend()), false)[0]->toArray(),
        );
    }

    public function test_grouped_sums_and_averages_match_the_ungrouped_aggregates_of_each_group(): void
    {
        $input = schema(str_schema('k'), float_schema('v'));
        $batches = [
            [['k' => 'big', 'v' => 1.0e16], ['k' => 'small', 'v' => 0.1], ['k' => 'big', 'v' => 1.0]],
            [['k' => 'small', 'v' => 0.2], ['k' => 'big', 'v' => -1.0e16], ['k' => 'small', 'v' => 0.3]],
        ];
        $aggregators = new Aggregators(
            sum(ref('v')),
            average(ref('v'), scale: 4),
            sum(ref('v')->as('exact'), exact: true),
        );
        $groupBy = new GroupBy(refs('k'), $aggregators);
        $groups = new AggregatedGroups($groupBy, GroupByShape::of($groupBy, $input), flow_context(config())->backend());
        $expected = [];

        foreach (['big', 'small'] as $key) {
            $ungrouped = $aggregators->fresh();

            foreach ($batches as $batch) {
                $rows = array_to_rows(
                    array_values(array_filter($batch, static fn(array $row): bool => $row['k'] === $key)),
                    $input,
                );

                foreach ($ungrouped as $aggregator) {
                    $aggregator->aggregate($rows, array_keys($rows->toArray()), flow_context(config())->backend());
                }
            }

            $expected[] = [
                'k' => $key,
                ...array_combine(
                    ['v_sum', 'v_avg', 'exact'],
                    array_map(
                        static fn(AggregatingFunction $a): mixed => $a->value(),
                        iterator_to_array($ungrouped, false),
                    ),
                ),
            ];
        }

        foreach ($batches as $batch) {
            $groups->accumulate(array_to_rows($batch, $input), flow_context(config())->backend());
        }

        static::assertSame(
            $expected,
            iterator_to_array($groups->flush(1000, flow_context(config())->backend()), false)[0]->toArray(),
        );
    }

    public function test_a_grouped_int_sum_past_the_int_range_promotes_to_float_as_the_ungrouped_sum_does(): void
    {
        $input = schema(str_schema('k'), int_schema('v'));
        $groupBy = new GroupBy(refs('k'), new Aggregators(sum(ref('v')), average(ref('v'))));
        $groups = new AggregatedGroups($groupBy, GroupByShape::of($groupBy, $input), flow_context(config())->backend());

        $groups->accumulate(array_to_rows([
            ['k' => 'a', 'v' => PHP_INT_MAX],
            ['k' => 'a', 'v' => 1],
        ], $input), flow_context(config())->backend());

        static::assertSame(
            [['k' => 'a', 'v_sum' => 9.223372036854776E+18, 'v_avg' => 4.6116860184274E+18]],
            iterator_to_array($groups->flush(1000, flow_context(config())->backend()), false)[0]->toArray(),
        );
    }

    public function test_counts_take_every_non_null_value_of_any_type_and_count_without_a_column_takes_rows(): void
    {
        $input = schema(str_schema('k'), str_schema('note', nullable: true));
        $groupBy = new GroupBy(refs('k'), new Aggregators(count(ref('note')), count()));
        $groups = new AggregatedGroups($groupBy, GroupByShape::of($groupBy, $input), flow_context(config())->backend());

        $groups->accumulate(array_to_rows([
            ['k' => 'a', 'note' => 'x'],
            ['k' => 'a', 'note' => null],
            ['k' => 'b', 'note' => 'not a number'],
        ], $input), flow_context(config())->backend());
        $groups->accumulate(array_to_rows([['k' => 'b']], schema(str_schema('k'))), flow_context(config())->backend());

        static::assertSame(
            [['k' => 'a', 'note_count' => 1, '_count' => 2], ['k' => 'b', 'note_count' => 1, '_count' => 2]],
            iterator_to_array($groups->flush(1000, flow_context(config())->backend()), false)[0]->toArray(),
        );
    }

    public function test_partial_aggregates_accumulate_on_the_grouped_path_of_the_aggregates_they_wrap(): void
    {
        $backend = new SpyBackend();
        $input = schema(str_schema('k'), float_schema('v'));
        $groupBy = new GroupBy(
            refs('k'),
            new Aggregators(
                new PartialAggregate(sum(ref('v'))),
                new PartialAggregate(average(ref('v'))),
                new PartialAggregate(count(ref('v'))),
            ),
        );

        new AggregatedGroups($groupBy, GroupByShape::of($groupBy, $input), $backend);

        static::assertSame([1, 1], [$backend->groupedTotalsCreated(), $backend->groupedCountsCreated()]);
    }

    public function test_partial_aggregates_flush_the_partials_the_ungrouped_aggregates_of_each_group_hold(): void
    {
        $input = schema(str_schema('k'), float_schema('v', nullable: true), str_schema('note', nullable: true));
        $batches = [
            [['k' => 'big', 'v' => 1.0e16, 'note' => 'a'], ['k' => 'small', 'v' => 0.1, 'note' => null]],
            [['k' => 'small', 'v' => null, 'note' => 'b'], ['k' => 'big', 'v' => -1.0e16, 'note' => 'c']],
            [['k' => 'big', 'v' => 1.0, 'note' => null], ['k' => 'nulls', 'v' => null, 'note' => null]],
        ];
        $aggregators = new Aggregators(
            new PartialAggregate(sum(ref('v'))),
            new PartialAggregate(average(ref('v'), scale: 4)),
            new PartialAggregate(count(ref('note'))),
            new PartialAggregate(count()),
            new PartialAggregate(sum(ref('v')->as('exact'), exact: true)),
        );
        $groupBy = new GroupBy(refs('k'), $aggregators);
        $groups = new AggregatedGroups($groupBy, GroupByShape::of($groupBy, $input), flow_context(config())->backend());
        $expected = [];

        foreach (['big', 'small', 'nulls'] as $key) {
            $ungrouped = $aggregators->fresh();

            foreach ($batches as $batch) {
                $rows = array_to_rows(
                    array_values(array_filter($batch, static fn(array $row): bool => $row['k'] === $key)),
                    $input,
                );

                foreach ($ungrouped as $aggregator) {
                    $aggregator->aggregate($rows, array_keys($rows->toArray()), flow_context(config())->backend());
                }
            }

            $expected[] = [
                'k' => $key,
                ...array_combine(
                    ['v_sum', 'v_avg', 'note_count', '_count', 'exact'],
                    array_map(
                        static fn(AggregatingFunction $a): mixed => $a->value(),
                        iterator_to_array($ungrouped, false),
                    ),
                ),
            ];
        }

        foreach ($batches as $batch) {
            $groups->accumulate(array_to_rows($batch, $input), flow_context(config())->backend());
        }

        static::assertSame(
            $expected,
            iterator_to_array($groups->flush(1000, flow_context(config())->backend()), false)[0]->toArray(),
        );
    }

    public function test_absorbing_earlier_groups_folds_them_in_front_and_appends_groups_only_they_held(): void
    {
        $input = schema(str_schema('k'), float_schema('v'), str_schema('note'));
        $groupBy = new GroupBy(
            refs('k'),
            new Aggregators(
                sum(ref('v')),
                average(ref('v'), scale: 4),
                count(ref('note')),
                string_agg(ref('note'), ','),
            ),
        );
        $shape = GroupByShape::of($groupBy, $input);
        $earlier = new AggregatedGroups($groupBy, $shape, flow_context(config())->backend());
        $later = new AggregatedGroups($groupBy, $shape, flow_context(config())->backend());
        $sequential = new AggregatedGroups($groupBy, $shape, flow_context(config())->backend());
        $first = array_to_rows([
            ['k' => 'only-earlier', 'v' => 5.0, 'note' => 'e'],
            ['k' => 'both', 'v' => 1.0e16, 'note' => 'a'],
        ], $input);
        $second = array_to_rows([
            ['k' => 'both', 'v' => 1.0, 'note' => 'b'],
            ['k' => 'both', 'v' => -1.0e16, 'note' => 'c'],
        ], $input);
        $earlier->accumulate($first, flow_context(config())->backend());
        $later->accumulate($second, flow_context(config())->backend());
        $sequential->accumulate($first, flow_context(config())->backend());
        $sequential->accumulate($second, flow_context(config())->backend());

        $later->absorbEarlier($earlier, [1, 0], flow_context(config())->backend());

        static::assertSame(
            [
                ['k' => 'both', 'v_sum' => 1.0, 'v_avg' => 0.3333, 'note_count' => 3, 'note_str_agg' => 'a,b,c'],
                ['k' => 'only-earlier', 'v_sum' => 5.0, 'v_avg' => 5.0, 'note_count' => 1, 'note_str_agg' => 'e'],
            ],
            iterator_to_array($later->flush(1000, flow_context(config())->backend()), false)[0]->toArray(),
        );
        static::assertEqualsCanonicalizing(
            iterator_to_array($sequential->flush(1000, flow_context(config())->backend()), false)[0]->toArray(),
            iterator_to_array($later->flush(1000, flow_context(config())->backend()), false)[0]->toArray(),
        );
    }
}
