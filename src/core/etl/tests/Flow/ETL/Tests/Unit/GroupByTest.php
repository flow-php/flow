<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy;
use Flow\ETL\Tests\Context\GroupByContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;

final class GroupByTest extends FlowTestCase
{
    public function test_group_by_missing_entry(): void
    {
        $groupBy = new GroupBy('type');
        $groupBy->aggregate(count());

        $result = GroupByContext::aggregated(
            $groupBy,
            flow_context(config()),
            rows(row(str_entry('type', 'a')), row(str_entry('not-type', 'b')), row(str_entry('type', 'c'))),
        );

        static::assertSame(
            [
                ['type' => 'a', '_count' => 1],
                ['type' => null, '_count' => 1],
                ['type' => 'c', '_count' => 1],
            ],
            $result->toArray(),
        );
        static::assertEquals(schema(str_schema('type', true), int_schema('_count')), $result->schema());
    }

    public function test_group_by_with_aggregation(): void
    {
        $group = new GroupBy('type');
        $group->aggregate(sum(ref('id')));

        $result = GroupByContext::aggregated(
            $group,
            flow_context(config()),
            rows(
                row(int_entry('id', 1), str_entry('type', 'a')),
                row(int_entry('id', 2), str_entry('type', 'b')),
                row(int_entry('id', 3), str_entry('type', 'c')),
                row(int_entry('id', 4), str_entry('type', 'a')),
                row(int_entry('id', 5), str_entry('type', 'd')),
            ),
        );

        static::assertSame(
            [
                ['type' => 'a', 'id_sum' => 5.0],
                ['type' => 'b', 'id_sum' => 2.0],
                ['type' => 'c', 'id_sum' => 3.0],
                ['type' => 'd', 'id_sum' => 5.0],
            ],
            $result->toArray(),
        );
        static::assertEquals(schema(str_schema('type', true), float_schema('id_sum', true)), $result->schema());
    }

    public function test_output_schema_declares_one_definition_per_aggregate(): void
    {
        $groupBy = new GroupBy('country');
        $groupBy->aggregate(sum(ref('age')), count(ref('age')));

        $input = schema(str_schema('country'), int_schema('age'));

        static::assertEquals(
            schema(str_schema('country', true), float_schema('age_sum', true), int_schema('age_count')),
            $groupBy->outputSchema($input, $groupBy->aggregations()->resolved($input)),
        );
    }

    public function test_group_by_with_empty_aggregations(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Aggregations can't be empty");
        $groupBy = new GroupBy();
        $groupBy->aggregate();
    }

    public function test_group_by_with_pivoting(): void
    {
        $group = new GroupBy(ref('product'));
        $group->aggregate(sum(ref('amount')));
        $group->pivot(ref('country'));

        static::assertEquals(
            rows(
                row(
                    str_entry('product', 'Banana'),
                    float_entry('Canada', 2000.0),
                    float_entry('China', 400.0),
                    null_entry('Mexico'),
                    float_entry('USA', 1000.0),
                ),
                row(
                    str_entry('product', 'Beans'),
                    null_entry('Canada'),
                    float_entry('China', 1500.0),
                    float_entry('Mexico', 2000.0),
                    float_entry('USA', 1600.0),
                ),
                row(
                    str_entry('product', 'Carrots'),
                    float_entry('Canada', 2000.0),
                    float_entry('China', 1200.0),
                    null_entry('Mexico'),
                    float_entry('USA', 1500.0),
                ),
                row(
                    str_entry('product', 'Orange'),
                    null_entry('Canada'),
                    float_entry('China', 4000.0),
                    null_entry('Mexico'),
                    float_entry('USA', 4000.0),
                ),
            ),
            GroupByContext::aggregated(
                $group,
                flow_context(config()),
                rows(
                    row(str_entry('product', 'Banana'), int_entry('amount', 1000), str_entry('country', 'USA')),
                    row(str_entry('product', 'Carrots'), int_entry('amount', 1500), str_entry('country', 'USA')),
                    row(str_entry('product', 'Beans'), int_entry('amount', 1600), str_entry('country', 'USA')),
                    row(str_entry('product', 'Orange'), int_entry('amount', 2000), str_entry('country', 'USA')),
                    row(str_entry('product', 'Orange'), int_entry('amount', 2000), str_entry('country', 'USA')),
                    row(str_entry('product', 'Banana'), int_entry('amount', 400), str_entry('country', 'China')),
                    row(str_entry('product', 'Carrots'), int_entry('amount', 1200), str_entry('country', 'China')),
                    row(str_entry('product', 'Beans'), int_entry('amount', 1500), str_entry('country', 'China')),
                    row(str_entry('product', 'Orange'), int_entry('amount', 4000), str_entry('country', 'China')),
                    row(str_entry('product', 'Banana'), int_entry('amount', 2000), str_entry('country', 'Canada')),
                    row(str_entry('product', 'Carrots'), int_entry('amount', 2000), str_entry('country', 'Canada')),
                    row(str_entry('product', 'Beans'), int_entry('amount', 2000), str_entry('country', 'Mexico')),
                ),
            )->sortBy(ref('product')),
        );
    }

    public function test_group_by_with_pivoting_with_null_pivot_column(): void
    {
        $group = new GroupBy(ref('product'));
        $group->aggregate(sum(ref('amount')));
        $group->pivot(ref('country'));

        static::assertEquals(
            rows(
                row(str_entry('product', 'Apple'), null_entry('USA')),
                row(str_entry('product', 'Banana'), float_entry('USA', 1000.0)),
            ),
            GroupByContext::aggregated(
                $group,
                flow_context(config()),
                rows(
                    row(str_entry('product', 'Banana'), str_entry('country', 'USA'), int_entry('amount', 1000)),
                    row(str_entry('product', 'Apple'), str_entry('country', null), int_entry('amount', 400)),
                ),
            )->sortBy(ref('product')),
        );
    }

    public function test_a_group_column_absent_from_the_schema_stays_value_derived(): void
    {
        $groupBy = new GroupBy('missing');
        $groupBy->aggregate(count());

        $result = GroupByContext::aggregated(
            $groupBy,
            flow_context(config()),
            rows(row(str_entry('type', 'a')), row(str_entry('type', 'b'))),
        );

        static::assertSame([['missing' => null, '_count' => 2]], $result->toArray());
        static::assertTrue($result->schema()->get('missing')->isNullable());
    }
}
