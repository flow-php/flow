<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\GroupBy;

use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\GroupByShape;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;

final class GroupByShapeTest extends FlowTestCase
{
    public function test_it_carries_the_input_the_bound_aggregators_and_the_output_schema(): void
    {
        $groupBy = new GroupBy('country');
        $groupBy->aggregate(sum(ref('age')));

        $input = schema(str_schema('country'), int_schema('age'));
        $shape = GroupByShape::of($groupBy, $input);

        static::assertEquals($input, $shape->input);
        static::assertEquals($groupBy->aggregations()->resolved($input), $shape->aggregators);
        static::assertEquals(schema(str_schema('country'), float_schema('age_sum', nullable: true)), $shape->output);
    }

    public function test_it_refuses_a_group_column_the_input_does_not_declare(): void
    {
        $groupBy = new GroupBy('missing');
        $groupBy->aggregate(sum(ref('age')));

        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for entry "missing" not found');

        GroupByShape::of($groupBy, schema(str_schema('country'), int_schema('age')));
    }

    public function test_it_refuses_an_aggregation_over_a_column_the_input_does_not_declare(): void
    {
        $groupBy = new GroupBy('country');
        $groupBy->aggregate(sum(ref('missing')));

        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for entry "missing" not found');

        GroupByShape::of($groupBy, schema(str_schema('country'), int_schema('age')));
    }
}
