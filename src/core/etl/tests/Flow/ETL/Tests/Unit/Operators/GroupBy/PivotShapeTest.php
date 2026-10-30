<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\GroupBy;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Operators\GroupBy\Pivot;
use Flow\ETL\Operators\GroupBy\PivotShape;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\min;
use function Flow\ETL\DSL\pivot_values;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;

final class PivotShapeTest extends FlowTestCase
{
    public function test_it_resolves_the_aggregation_against_the_input_schema(): void
    {
        $shape = PivotShape::of(
            new GroupBy(refs(ref('k')), new Aggregators(min(ref('a'))), new Pivot(ref('p'), pivot_values('p1', 'p2'))),
            schema(str_schema('k'), str_schema('p'), int_schema('a')),
        );

        static::assertSame('integer', $shape->output->get('p1')->type()->toString());
        static::assertTrue($shape->output->get('p1')->isNullable());
        static::assertSame('integer', $shape->output->get('p2')->type()->toString());
        static::assertTrue($shape->output->get('p2')->isNullable());
        static::assertSame('?integer', $shape->aggregation->returns()->toString());
    }

    public function test_it_declares_one_column_per_pivot_value(): void
    {
        static::assertSame(
            ['k', 'p1', 'p2', 'p3'],
            PivotShape::of(
                new GroupBy(
                    refs(ref('k')),
                    new Aggregators(sum(ref('a'))),
                    new Pivot(ref('p'), pivot_values('p1', 'p2', 'p3')),
                ),
                schema(str_schema('k'), str_schema('p'), int_schema('a')),
            )->output->names(),
        );
    }

    public function test_it_refuses_a_group_by_that_does_not_pivot(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('GroupBy does not pivot, there is no pivot shape to derive');

        PivotShape::of(
            new GroupBy(refs(ref('k')), new Aggregators(sum(ref('a')))),
            schema(str_schema('k'), int_schema('a')),
        );
    }

    public function test_it_refuses_an_unknown_group_by_column(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for column "nope" not found');

        PivotShape::of(
            new GroupBy(refs(ref('nope')), new Aggregators(sum(ref('a'))), new Pivot(ref('p'), pivot_values('p1'))),
            schema(str_schema('k'), str_schema('p'), int_schema('a')),
        );
    }

    public function test_it_refuses_an_unknown_aggregate_column(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for column "nope" not found');

        PivotShape::of(
            new GroupBy(refs(ref('k')), new Aggregators(min(ref('nope'))), new Pivot(ref('p'), pivot_values('p1'))),
            schema(str_schema('k'), str_schema('p'), int_schema('a')),
        );
    }
}
