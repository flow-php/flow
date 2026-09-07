<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\GroupBy;

use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\AggregatedGroups;
use Flow\ETL\GroupBy\GroupByShape;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;

final class AggregatedGroupsTest extends FlowTestCase
{
    public function test_it_accumulates_rows_of_the_same_key_into_one_group(): void
    {
        $groupBy = new GroupBy('country');
        $groupBy->aggregate(sum(ref('amount')));

        $input = schema(str_schema('country'), int_schema('amount'));
        $groups = new AggregatedGroups($groupBy, GroupByShape::of($groupBy, $input));

        $groups->accumulate(
            rows($input, row(['country' => 'PL', 'amount' => 10]), row(['country' => 'DE', 'amount' => 20])),
            flow_context(config()),
        );
        $groups->accumulate(rows($input, row(['country' => 'PL', 'amount' => 30])), flow_context(config()));

        /** @var list<Rows> $batches */
        $batches = iterator_to_array($groups->flush(1000));

        static::assertCount(1, $batches);
        static::assertSame(
            [
                ['country' => 'PL', 'amount_sum' => 40.0],
                ['country' => 'DE', 'amount_sum' => 20.0],
            ],
            $batches[0]->toArray(),
        );
    }

    public function test_the_flushed_batch_carries_the_shape_output_schema(): void
    {
        $groupBy = new GroupBy('country');
        $groupBy->aggregate(sum(ref('amount')));

        $input = schema(str_schema('country'), int_schema('amount'));
        $shape = GroupByShape::of($groupBy, $input);
        $groups = new AggregatedGroups($groupBy, $shape);

        $groups->accumulate(rows($input, row(['country' => 'PL', 'amount' => 10])), flow_context(config()));

        /** @var list<Rows> $batches */
        $batches = iterator_to_array($groups->flush(1000));

        static::assertEquals($shape->output, $batches[0]->schema());
        static::assertEquals(schema(str_schema('country'), float_schema('amount_sum', nullable: true)), $shape->output);
    }

    public function test_it_splits_the_flushed_groups_into_batches_of_the_requested_size(): void
    {
        $groupBy = new GroupBy('country');
        $groupBy->aggregate(sum(ref('amount')));

        $input = schema(str_schema('country'), int_schema('amount'));
        $groups = new AggregatedGroups($groupBy, GroupByShape::of($groupBy, $input));

        $groups->accumulate(
            rows(
                $input,
                row(['country' => 'PL', 'amount' => 10]),
                row(['country' => 'DE', 'amount' => 20]),
                row(['country' => 'FR', 'amount' => 30]),
            ),
            flow_context(config()),
        );

        static::assertSame(
            [2, 1],
            array_map(static fn(Rows $batch): int => $batch->count(), iterator_to_array($groups->flush(2))),
        );
    }

    public function test_flushing_without_accumulating_yields_nothing(): void
    {
        $groupBy = new GroupBy('country');
        $groupBy->aggregate(sum(ref('amount')));

        $groups = new AggregatedGroups($groupBy, GroupByShape::of($groupBy, schema(
            str_schema('country'),
            int_schema('amount'),
        )));

        static::assertSame([], iterator_to_array($groups->flush(1000)));
    }

    public function test_a_row_missing_a_nullable_group_key_lands_in_the_null_group(): void
    {
        $groupBy = new GroupBy('country');
        $groupBy->aggregate(sum(ref('amount')));

        $input = schema(str_schema('country', nullable: true), int_schema('amount'));
        $groups = new AggregatedGroups($groupBy, GroupByShape::of($groupBy, $input));

        $groups->accumulate(
            rows($input, row(['amount' => 10]), row(['country' => 'PL', 'amount' => 20]), row(['amount' => 5])),
            flow_context(config()),
        );

        /** @var list<Rows> $batches */
        $batches = iterator_to_array($groups->flush(1000));

        static::assertSame(
            [
                ['country' => null, 'amount_sum' => 15.0],
                ['country' => 'PL', 'amount_sum' => 20.0],
            ],
            $batches[0]->toArray(),
        );
    }
}
