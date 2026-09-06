<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\GroupBy;
use Flow\ETL\Tests\Context\GroupByContext;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
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
            rows(
                schema(str_schema('type', nullable: true), str_schema('not-type', nullable: true)),
                row(['type' => 'a']),
                row(['not-type' => 'b']),
                row(['type' => 'c']),
            ),
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
                schema(int_schema('id'), str_schema('type')),
                row(['id' => 1, 'type' => 'a']),
                row(['id' => 2, 'type' => 'b']),
                row(['id' => 3, 'type' => 'c']),
                row(['id' => 4, 'type' => 'a']),
                row(['id' => 5, 'type' => 'd']),
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
        static::assertEquals(schema(str_schema('type'), float_schema('id_sum', true)), $result->schema());
    }

    public function test_output_schema_declares_one_definition_per_aggregate(): void
    {
        $groupBy = new GroupBy('country');
        $groupBy->aggregate(sum(ref('age')), count(ref('age')));

        $input = schema(str_schema('country'), int_schema('age'));

        static::assertEquals(
            schema(str_schema('country'), float_schema('age_sum', true), int_schema('age_count')),
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

    public function test_a_pivot_declares_its_output_schema(): void
    {
        $group = new GroupBy(ref('product'));
        $group->aggregate(sum(ref('amount')));
        $group->pivot(ref('country'));

        $schema = iterator_to_array($group->pivotResult(
            (static function (): Generator {
                yield rows(
                    schema(str_schema('product'), str_schema('country'), int_schema('amount')),
                    row(['product' => 'Banana', 'country' => 'USA', 'amount' => 1000]),
                );
            })(),
            flow_context(config()),
        ))[0]->schema();

        static::assertSame(['product', 'USA'], $schema->references()->names());
        static::assertFalse($schema->get('product')->isNullable());
        static::assertTrue($schema->get('USA')->isNullable());
        static::assertSame('float', $schema->get('USA')->type()->toString());
    }

    public function test_a_pivot_over_an_empty_input_yields_nothing(): void
    {
        $group = new GroupBy(ref('product'));
        $group->aggregate(sum(ref('amount')));
        $group->pivot(ref('country'));

        static::assertSame(
            [],
            iterator_to_array($group->pivotResult((static fn(): Generator => yield from [])(), flow_context(config()))),
        );
    }

    public function test_a_falsy_pivot_value_keeps_its_column(): void
    {
        $group = new GroupBy(ref('product'));
        $group->aggregate(sum(ref('amount')));
        $group->pivot(ref('country'));

        $result = iterator_to_array($group->pivotResult(
            (static function (): Generator {
                yield rows(
                    schema(str_schema('product'), str_schema('country'), int_schema('amount')),
                    row(['product' => 'Banana', 'country' => '0', 'amount' => 7]),
                    row(['product' => 'Banana', 'country' => 'USA', 'amount' => 3]),
                );
            })(),
            flow_context(config()),
        ))[0];

        static::assertSame(['product', '0', 'USA'], $result->schema()->references()->names());
        static::assertSame([['product' => 'Banana', '0' => 7.0, 'USA' => 3.0]], $result->toArray());
    }

    public function test_group_by_with_pivoting(): void
    {
        $group = new GroupBy(ref('product'));
        $group->aggregate(sum(ref('amount')));
        $group->pivot(ref('country'));

        static::assertEquals(
            rows(
                schema(
                    str_schema('product'),
                    // PivotSchema declares every pivot column optional: the yield loop writes null
                    // for a combination it never saw, so nullability cannot depend on the data
                    float_schema('Canada', nullable: true),
                    float_schema('China', nullable: true),
                    float_schema('Mexico', nullable: true),
                    float_schema('USA', nullable: true),
                ),
                row(['product' => 'Banana', 'Canada' => 2000.0, 'China' => 400.0, 'Mexico' => null, 'USA' => 1000.0]),
                row(['product' => 'Beans', 'Canada' => null, 'China' => 1500.0, 'Mexico' => 2000.0, 'USA' => 1600.0]),
                row(['product' => 'Carrots', 'Canada' => 2000.0, 'China' => 1200.0, 'Mexico' => null, 'USA' => 1500.0]),
                row(['product' => 'Orange', 'Canada' => null, 'China' => 4000.0, 'Mexico' => null, 'USA' => 4000.0]),
            ),
            GroupByContext::aggregated(
                $group,
                flow_context(config()),
                rows(
                    schema(str_schema('product'), int_schema('amount'), str_schema('country')),
                    row(['product' => 'Banana', 'amount' => 1000, 'country' => 'USA']),
                    row(['product' => 'Carrots', 'amount' => 1500, 'country' => 'USA']),
                    row(['product' => 'Beans', 'amount' => 1600, 'country' => 'USA']),
                    row(['product' => 'Orange', 'amount' => 2000, 'country' => 'USA']),
                    row(['product' => 'Orange', 'amount' => 2000, 'country' => 'USA']),
                    row(['product' => 'Banana', 'amount' => 400, 'country' => 'China']),
                    row(['product' => 'Carrots', 'amount' => 1200, 'country' => 'China']),
                    row(['product' => 'Beans', 'amount' => 1500, 'country' => 'China']),
                    row(['product' => 'Orange', 'amount' => 4000, 'country' => 'China']),
                    row(['product' => 'Banana', 'amount' => 2000, 'country' => 'Canada']),
                    row(['product' => 'Carrots', 'amount' => 2000, 'country' => 'Canada']),
                    row(['product' => 'Beans', 'amount' => 2000, 'country' => 'Mexico']),
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
                schema(str_schema('product'), float_schema('USA', nullable: true)),
                row(['product' => 'Apple', 'USA' => null]),
                row(['product' => 'Banana', 'USA' => 1000.0]),
            ),
            GroupByContext::aggregated(
                $group,
                flow_context(config()),
                rows(
                    schema(str_schema('product'), str_schema('country', nullable: true), int_schema('amount')),
                    row(['product' => 'Banana', 'country' => 'USA', 'amount' => 1000]),
                    row(['product' => 'Apple', 'country' => null, 'amount' => 400]),
                ),
            )->sortBy(ref('product')),
        );
    }

    /**
     * Inventing a group column the batch schema does not declare would write a value into row storage
     * that no schema-driven reader can see, so the aggregate refuses at bind instead.
     */
    public function test_a_group_column_absent_from_the_schema_is_refused_at_bind(): void
    {
        $groupBy = new GroupBy('missing');
        $groupBy->aggregate(count());

        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for entry "missing" not found');

        GroupByContext::aggregated(
            $groupBy,
            flow_context(config()),
            rows(schema(str_schema('type')), row(['type' => 'a']), row(['type' => 'b'])),
        );
    }

    public function test_a_not_null_group_key_stays_not_null_in_the_output_schema(): void
    {
        $groupBy = new GroupBy('country');
        $groupBy->aggregate(count());

        $input = schema(str_schema('country'), int_schema('age'));

        static::assertEquals(
            schema(str_schema('country'), int_schema('_count')),
            $groupBy->outputSchema($input, $groupBy->aggregations()->resolved($input)),
        );
    }

    public function test_a_nullable_group_key_stays_nullable_in_the_output_schema(): void
    {
        $groupBy = new GroupBy('country');
        $groupBy->aggregate(count());

        $input = schema(str_schema('country', nullable: true), int_schema('age'));

        static::assertEquals(
            schema(str_schema('country', nullable: true), int_schema('_count')),
            $groupBy->outputSchema($input, $groupBy->aggregations()->resolved($input)),
        );
    }

    public function test_key_values_throws_when_a_row_lacks_a_not_null_key(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "country" does not exist. Did you mean one of the following? ["age"]');

        (new GroupBy('country'))->keyValues(row(['age' => 20]), schema(str_schema('country'), int_schema('age')));
    }

    public function test_key_values_substitutes_null_when_a_row_lacks_a_nullable_key(): void
    {
        static::assertSame(
            ['country' => null],
            iterator_to_array((new GroupBy('country'))->keyValues(
                row(['age' => 20]),
                schema(str_schema('country', nullable: true), int_schema('age')),
            )),
        );
    }
}
