<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\GroupBy;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Tests\Double\UnenumerableSum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ListColumnsMother;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\sum;
use function iterator_to_array;

final class AggregatorsTest extends FlowTestCase
{
    public function test_fresh_drops_state(): void
    {
        $aggregators = new Aggregators(count(), sum(ref('v')));
        $aggregators->aggregate(
            array_to_rows([['v' => 1], ['v' => 2]], schema(int_schema('v'))),
            [0, 1],
            flow_context()->backend(),
        );

        $fresh = $aggregators->fresh();

        static::assertSame(
            [2, 3.0],
            array_map(static fn(AggregatingFunction $a): mixed => $a->value(), iterator_to_array($aggregators, false)),
        );
        static::assertSame(
            [0, null],
            array_map(static fn(AggregatingFunction $a): mixed => $a->value(), iterator_to_array($fresh, false)),
        );
    }

    public function test_first_is_refused_without_aggregators(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Aggregators are empty.');

        (new Aggregators())->first();
    }

    public function test_references_are_empty_without_aggregators(): void
    {
        static::assertSame([], (new Aggregators())->references());
    }

    public function test_references_deduplicate_by_base_entry(): void
    {
        static::assertEquals(
            [ref('amount')],
            (new Aggregators(sum(ref('amount')), average(ref('amount'))))->references(),
        );
    }

    public function test_references_union_all_aggregators(): void
    {
        static::assertEquals(
            [ref('order_id'), ref('discount')],
            (new Aggregators(count(ref('order_id')), sum(ref('discount'))))->references(),
        );
    }

    public function test_references_are_null_when_any_aggregator_cannot_enumerate_them(): void
    {
        static::assertNull(
            (new Aggregators(count(ref('order_id')), new UnenumerableSum(ref('discount'))))->references(),
        );
    }

    public function test_resolved_refuses_an_expand_in_an_aggregate(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'aggregate() cannot contain array_expand(), it turns one row into many rows. Expand with withColumn() first, then use the new column.',
        );

        (new Aggregators(new UnenumerableSum(ref('n'), ref('flags')->expand()->equals(lit(true)))))->resolved(
            ListColumnsMother::numberAndFlagsSchema(),
        );
    }
}
