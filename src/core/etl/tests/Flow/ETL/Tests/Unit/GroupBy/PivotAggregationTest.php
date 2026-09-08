<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\GroupBy;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\PivotAggregation;
use Flow\ETL\GroupBy\PivotShape;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\min;
use function Flow\ETL\DSL\pivot_values;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;

final class PivotAggregationTest extends FlowTestCase
{
    public function test_throws_when_batch_size_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0, given: 0');

        // @mago-ignore analysis:invalid-argument
        new PivotAggregation(0);
    }

    public function test_a_bound_aggregation_pivots_without_reading_a_schema_from_the_batches(): void
    {
        $groupBy = new GroupBy(ref('product'));
        $groupBy->pivot(ref('country'), pivot_values('USA', 'PL'));
        $groupBy->aggregate(min(ref('amount')));

        $input = schema(str_schema('product'), str_schema('country'), int_schema('amount'));

        /** @var list<Rows> $batches */
        $batches = iterator_to_array((new PivotAggregation())->aggregateBound(
            (static function () use ($input): Generator {
                yield rows($input, row(['product' => 'Banana', 'country' => 'USA', 'amount' => 30]));
                yield rows($input, row(['product' => 'Banana', 'country' => 'USA', 'amount' => 10]));
            })(),
            flow_context(config()),
            $groupBy,
            PivotShape::of($groupBy, $input),
        ));

        static::assertSame([['product' => 'Banana', 'USA' => 10, 'PL' => null]], $batches[0]->toArray());
    }

    public function test_a_bound_aggregation_over_an_empty_stream_yields_nothing(): void
    {
        $groupBy = new GroupBy(ref('product'));
        $groupBy->pivot(ref('country'), pivot_values('USA'));
        $groupBy->aggregate(sum(ref('amount')));

        static::assertSame(
            [],
            iterator_to_array((new PivotAggregation())->aggregateBound(
                (static fn(): Generator => yield from [])(),
                flow_context(config()),
                $groupBy,
                PivotShape::of($groupBy, schema(str_schema('product'), str_schema('country'), int_schema('amount'))),
            )),
        );
    }

    public function test_it_splits_the_result_into_batches_of_the_requested_size(): void
    {
        $groupBy = new GroupBy(ref('product'));
        $groupBy->pivot(ref('country'), pivot_values('USA'));
        $groupBy->aggregate(sum(ref('amount')));

        $input = schema(str_schema('product'), str_schema('country'), int_schema('amount'));

        static::assertCount(
            2,
            iterator_to_array((new PivotAggregation(2))->aggregateBound(
                (static function () use ($input): Generator {
                    yield rows(
                        $input,
                        row(['product' => 'Banana', 'country' => 'USA', 'amount' => 1]),
                        row(['product' => 'Apple', 'country' => 'USA', 'amount' => 2]),
                        row(['product' => 'Cherry', 'country' => 'USA', 'amount' => 3]),
                    );
                })(),
                flow_context(config()),
                $groupBy,
                PivotShape::of($groupBy, $input),
            )),
        );
    }
}
