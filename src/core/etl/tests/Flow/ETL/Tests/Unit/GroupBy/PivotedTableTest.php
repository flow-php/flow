<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\GroupBy;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\PivotedTableMother;

use function array_map;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\min;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\sum;

final class PivotedTableTest extends FlowTestCase
{
    public function test_it_accumulates_rows_of_the_same_key_across_batches(): void
    {
        $table = PivotedTableMother::of(sum(ref('amount')), 'USA', 'PL');

        $table->accumulate(
            rows(
                PivotedTableMother::schema(),
                row(['product' => 'Banana', 'country' => 'USA', 'amount' => 10]),
                row(['product' => 'Banana', 'country' => 'PL', 'amount' => 20]),
            ),
            flow_context(config()),
        );
        $table->accumulate(
            rows(PivotedTableMother::schema(), row(['product' => 'Banana', 'country' => 'USA', 'amount' => 5])),
            flow_context(config()),
        );

        /** @var list<Rows> $batches */
        $batches = iterator_to_array($table->flush(1000, flow_context(config())));

        static::assertCount(1, $batches);
        static::assertSame([['product' => 'Banana', 'USA' => 15.0, 'PL' => 20.0]], $batches[0]->toArray());
    }

    public function test_a_pivot_value_never_seen_becomes_null(): void
    {
        $table = PivotedTableMother::of(sum(ref('amount')), 'USA', 'PL');

        $table->accumulate(
            rows(PivotedTableMother::schema(), row(['product' => 'Banana', 'country' => 'USA', 'amount' => 10])),
            flow_context(config()),
        );

        /** @var list<Rows> $batches */
        $batches = iterator_to_array($table->flush(1000, flow_context(config())));

        static::assertSame([['product' => 'Banana', 'USA' => 10.0, 'PL' => null]], $batches[0]->toArray());
    }

    public function test_a_null_pivot_value_keeps_the_group_row_and_feeds_no_column(): void
    {
        $input = PivotedTableMother::schema(nullableCountry: true);
        $table = PivotedTableMother::boundTo($input, sum(ref('amount')), 'USA');

        $table->accumulate(
            rows($input, row(['product' => 'Banana', 'country' => null, 'amount' => 10])),
            flow_context(config()),
        );

        /** @var list<Rows> $batches */
        $batches = iterator_to_array($table->flush(1000, flow_context(config())));

        static::assertSame([['product' => 'Banana', 'USA' => null]], $batches[0]->toArray());
    }

    public function test_a_pivot_value_outside_the_declared_values_is_discarded(): void
    {
        $table = PivotedTableMother::of(sum(ref('amount')), 'USA');

        $table->accumulate(
            rows(
                PivotedTableMother::schema(),
                row(['product' => 'Banana', 'country' => 'USA', 'amount' => 30]),
                row(['product' => 'Banana', 'country' => 'DE', 'amount' => 99]),
            ),
            flow_context(config()),
        );

        /** @var list<Rows> $batches */
        $batches = iterator_to_array($table->flush(1000, flow_context(config())));

        // the declared pivot values define the output; an undeclared one accumulates and is then dropped
        static::assertSame([['product' => 'Banana', 'USA' => 30.0]], $batches[0]->toArray());
    }

    public function test_it_splits_the_flushed_rows_into_batches_of_the_requested_size(): void
    {
        $table = PivotedTableMother::of(sum(ref('amount')), 'USA');

        $table->accumulate(
            rows(
                PivotedTableMother::schema(),
                row(['product' => 'Banana', 'country' => 'USA', 'amount' => 10]),
                row(['product' => 'Apple', 'country' => 'USA', 'amount' => 20]),
                row(['product' => 'Cherry', 'country' => 'USA', 'amount' => 30]),
            ),
            flow_context(config()),
        );

        static::assertSame(
            [2, 1],
            array_map(
                static fn(Rows $batch): int => $batch->count(),
                iterator_to_array($table->flush(2, flow_context(config()))),
            ),
        );
    }

    public function test_flushing_with_a_batch_size_below_one_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0, given: 0');

        // @mago-ignore analysis:invalid-argument
        iterator_to_array(PivotedTableMother::of(sum(ref('amount')), 'USA')->flush(0, flow_context(config())));
    }

    public function test_flushing_without_accumulating_yields_nothing(): void
    {
        static::assertSame(
            [],
            iterator_to_array(PivotedTableMother::of(sum(ref('amount')), 'USA')->flush(1000, flow_context(config()))),
        );
    }

    public function test_an_argument_typed_aggregate_keeps_its_resolved_column_type(): void
    {
        $table = PivotedTableMother::of(min(ref('amount')), 'USA');

        $table->accumulate(
            rows(
                PivotedTableMother::schema(),
                row(['product' => 'Banana', 'country' => 'USA', 'amount' => 30]),
                row(['product' => 'Banana', 'country' => 'USA', 'amount' => 10]),
            ),
            flow_context(config()),
        );

        /** @var list<Rows> $batches */
        $batches = iterator_to_array($table->flush(1000, flow_context(config())));

        static::assertSame([['product' => 'Banana', 'USA' => 10]], $batches[0]->toArray());
        static::assertSame('integer', $batches[0]->schema()->get('USA')->type()->toString());
    }
}
