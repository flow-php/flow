<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\GroupBy;

use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Operators\GroupBy\GroupByShape;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;

final class GroupByShapeTest extends FlowTestCase
{
    public function test_it_carries_the_input_the_bound_aggregators_and_the_output_schema(): void
    {
        $groupBy = new GroupBy(refs('country'), new Aggregators(sum(ref('age'))));

        $input = schema(str_schema('country'), int_schema('age'));
        $shape = GroupByShape::of($groupBy, $input);

        static::assertEquals($input, $shape->input);
        static::assertEquals($groupBy->aggregations()->resolved($input), $shape->aggregators);
        static::assertEquals(schema(str_schema('country'), float_schema('age_sum', nullable: true)), $shape->output);
    }

    public function test_it_refuses_a_group_column_the_input_does_not_declare(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for column "missing" not found');

        GroupByShape::of(
            new GroupBy(refs('missing'), new Aggregators(sum(ref('age')))),
            schema(str_schema('country'), int_schema('age')),
        );
    }

    public function test_it_refuses_an_aggregation_over_a_column_the_input_does_not_declare(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for column "missing" not found');

        GroupByShape::of(
            new GroupBy(refs('country'), new Aggregators(sum(ref('missing')))),
            schema(str_schema('country'), int_schema('age')),
        );
    }
}
