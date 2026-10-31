<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Tests\Double\RowLessExtractor;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\discover_pivot_values;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\pivot_values;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;

final class GroupedDataFrameTest extends FlowTestCase
{
    public function test_a_grouped_aggregate_without_aggregations_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Aggregations can't be empty");

        df()
            ->read(from_array([['a' => 1]]))
            ->groupBy('a')
            ->aggregate();
    }

    public function test_a_frame_aggregate_without_aggregations_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Aggregations can't be empty");

        df()
            ->read(from_array([['a' => 1]]))
            ->aggregate([])
            ->fetch();
    }

    public function test_a_group_by_without_aggregate_leaves_the_frame_unchanged(): void
    {
        $frame = df()->read(from_array([['a' => 1], ['a' => 1]]));
        $frame->groupBy('a');

        static::assertSame([['a' => 1], ['a' => 1]], $frame->fetch()->toArray());
    }

    public function test_aggregate_by_places_the_given_group_by(): void
    {
        static::assertSame(
            [['a' => 1, 'v_sum' => 3.0]],
            df()
                ->read(from_array([['a' => 1, 'v' => 1], ['a' => 1, 'v' => 2]]))
                ->aggregateBy(new GroupBy(refs('a'), new Aggregators(sum(ref('v')))))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_discovered_pivot_values_refuse_a_source_that_cannot_be_read_twice(): void
    {
        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('cannot read its dataset twice');

        df()
            ->read(new RowLessExtractor(schema(str_schema('country'), str_schema('code'))))
            ->groupBy('country')
            ->pivot(ref('code'), discover_pivot_values());
    }

    public function test_declared_pivot_values_never_read_a_source_that_cannot_be_read_twice(): void
    {
        $extractor = new RowLessExtractor(schema(str_schema('country'), str_schema('code'), int_schema('amount')));

        static::assertSame(
            [],
            df()
                ->read($extractor)
                ->groupBy('country')
                ->pivot(ref('code'), pivot_values('PL'))
                ->aggregate(sum(ref('amount')))
                ->fetch()
                ->toArray(),
        );
        static::assertSame(1, $extractor->extractCalls);
    }
}
