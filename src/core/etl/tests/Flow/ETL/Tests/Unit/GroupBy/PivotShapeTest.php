<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\GroupBy;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\PivotShape;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\min;
use function Flow\ETL\DSL\pivot_values;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;

final class PivotShapeTest extends FlowTestCase
{
    public function test_it_resolves_the_aggregation_against_the_input_schema(): void
    {
        $groupBy = new GroupBy(ref('k'));
        $groupBy->pivot(ref('p'), pivot_values('p1', 'p2'));
        $groupBy->aggregate(min(ref('a')));

        $shape = PivotShape::of($groupBy, schema(str_schema('k'), str_schema('p'), int_schema('a')));

        static::assertSame('integer', $shape->output->get('p1')->type()->toString());
        static::assertTrue($shape->output->get('p1')->isNullable());
        static::assertSame('integer', $shape->output->get('p2')->type()->toString());
        static::assertTrue($shape->output->get('p2')->isNullable());
        static::assertSame('?integer', $shape->aggregation->returns()->toString());
    }

    public function test_it_declares_one_column_per_pivot_value(): void
    {
        $groupBy = new GroupBy(ref('k'));
        $groupBy->pivot(ref('p'), pivot_values('p1', 'p2', 'p3'));
        $groupBy->aggregate(sum(ref('a')));

        static::assertSame(
            ['k', 'p1', 'p2', 'p3'],
            PivotShape::of($groupBy, schema(
                str_schema('k'),
                str_schema('p'),
                int_schema('a'),
            ))->output->references()->names(),
        );
    }

    public function test_it_refuses_a_group_by_that_does_not_pivot(): void
    {
        $groupBy = new GroupBy(ref('k'));
        $groupBy->aggregate(sum(ref('a')));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('GroupBy does not pivot, there is no pivot shape to derive');

        PivotShape::of($groupBy, schema(str_schema('k'), int_schema('a')));
    }

    public function test_it_refuses_an_unknown_group_by_column(): void
    {
        $groupBy = new GroupBy(ref('nope'));
        $groupBy->pivot(ref('p'), pivot_values('p1'));
        $groupBy->aggregate(sum(ref('a')));

        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for entry "nope" not found');

        PivotShape::of($groupBy, schema(str_schema('k'), str_schema('p'), int_schema('a')));
    }

    public function test_it_refuses_an_unknown_aggregate_column(): void
    {
        $groupBy = new GroupBy(ref('k'));
        $groupBy->pivot(ref('p'), pivot_values('p1'));
        $groupBy->aggregate(min(ref('nope')));

        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for entry "nope" not found');

        PivotShape::of($groupBy, schema(str_schema('k'), str_schema('p'), int_schema('a')));
    }
}
