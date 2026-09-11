<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\GroupBy;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy\PivotSchema;
use Flow\ETL\Row\References;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;

final class PivotSchemaTest extends FlowTestCase
{
    public function test_group_by_columns_keep_the_declaration_the_input_gave_them(): void
    {
        static::assertEquals(
            schema(str_schema('product'), float_schema('USA', nullable: true)),
            (new PivotSchema())->of(
                schema(str_schema('product'), str_schema('country'), int_schema('amount')),
                References::init(ref('product')),
                ['USA'],
                sum(ref('amount')),
            ),
        );
    }

    public function test_every_pivot_column_is_typed_from_the_aggregate_and_declared_nullable(): void
    {
        $output = (new PivotSchema())->of(
            schema(str_schema('product'), str_schema('country'), int_schema('amount')),
            References::init(ref('product')),
            ['USA', 'Canada'],
            sum(ref('amount')),
        );

        foreach (['USA', 'Canada'] as $column) {
            static::assertTrue($output->get($column)->isNullable(), $column);
            static::assertSame('float', $output->get($column)->type()->toString(), $column);
        }
    }

    public function test_an_integer_pivot_value_names_its_column_by_its_own_digits(): void
    {
        static::assertSame(
            ['product', '0', '7'],
            (new PivotSchema())
                ->of(
                    schema(str_schema('product'), int_schema('year'), int_schema('amount')),
                    References::init(ref('product')),
                    [0, 7],
                    sum(ref('amount')),
                )
                ->references()
                ->names(),
        );
    }

    public function test_a_pivot_value_colliding_with_a_group_by_column_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Pivot value "product" collides with the group-by column of the same name');

        (new PivotSchema())->of(
            schema(str_schema('product'), int_schema('amount')),
            References::init(ref('product')),
            ['product'],
            sum(ref('amount')),
        );
    }

    public function test_a_pivot_without_values_declares_only_the_group_by_columns(): void
    {
        static::assertEquals(
            schema(str_schema('product')),
            (new PivotSchema())->of(
                schema(str_schema('product'), int_schema('amount')),
                References::init(ref('product')),
                [],
                sum(ref('amount')),
            ),
        );
    }
}
