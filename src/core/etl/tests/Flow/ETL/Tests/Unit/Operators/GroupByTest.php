<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators;

use DateTimeImmutable;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Operators\GroupBy\GroupKey;
use Flow\ETL\Operators\GroupBy\Pivot;
use Flow\ETL\Operators\GroupBy\PivotShape;
use Flow\ETL\Operators\Processor\PivotProcessor;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\GroupByContext;
use Flow\ETL\Tests\Context\SortedRows;
use Flow\ETL\Tests\Double\UnenumerableSum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use Generator;

use function array_map;
use function array_unique;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\min;
use function Flow\ETL\DSL\pivot_values;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\uuid_schema;
use function iterator_to_array;

final class GroupByTest extends FlowTestCase
{
    public function test_describe_round_trips(): void
    {
        $groupBy = new GroupBy(refs('k'), new Aggregators(sum(ref('v')), count(ref('v'))));

        $rebuilt = DescribeRoundTrip::rebuilt($groupBy);

        static::assertEquals($groupBy->refs(), $rebuilt->refs());
        static::assertFalse($rebuilt->isGlobal());
    }

    public function test_a_pivot_survives_the_round_trip(): void
    {
        static::assertSame(
            ['a', 1],
            DescribeRoundTrip::rebuilt(
                new GroupBy(refs('k'), new Aggregators(sum(ref('v'))), new Pivot(ref('p'), pivot_values('a', 1))),
            )->describe()?->data['pivot']['values'] ?? null,
        );
    }

    public function test_group_by_missing_entry(): void
    {
        $result = GroupByContext::aggregated(
            new GroupBy(refs('type'), new Aggregators(count())),
            flow_context(config()),
            array_to_rows(
                [['type' => 'a'], ['not-type' => 'b'], ['type' => 'c']],
                schema(str_schema('type', nullable: true), str_schema('not-type', nullable: true)),
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
        $result = GroupByContext::aggregated(
            new GroupBy(refs('type'), new Aggregators(sum(ref('id')))),
            flow_context(config()),
            array_to_rows(
                [
                    ['id' => 1, 'type' => 'a'],
                    ['id' => 2, 'type' => 'b'],
                    ['id' => 3, 'type' => 'c'],
                    ['id' => 4, 'type' => 'a'],
                    ['id' => 5, 'type' => 'd'],
                ],
                schema(int_schema('id'), str_schema('type')),
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
        $groupBy = new GroupBy(refs('country'), new Aggregators(sum(ref('age')), count(ref('age'))));

        $input = schema(str_schema('country'), int_schema('age'));

        static::assertEquals(
            schema(str_schema('country'), float_schema('age_sum', true), int_schema('age_count')),
            $groupBy->outputSchema($input, $groupBy->aggregations()->resolved($input)),
        );
    }

    public function test_a_pivot_declares_its_output_schema(): void
    {
        $schema = iterator_to_array((new PivotProcessor(
            new GroupBy(
                refs(ref('product')),
                new Aggregators(sum(ref('amount'))),
                new Pivot(ref('country'), pivot_values('USA')),
            ),
            config()->grouping,
        ))->process(
            (static function (): Generator {
                yield array_to_rows(
                    [['product' => 'Banana', 'country' => 'USA', 'amount' => 1000]],
                    schema(str_schema('product'), str_schema('country'), int_schema('amount')),
                );
            })(),
            flow_context(config())->backend(),
        ))[0]->schema();

        static::assertSame(['product', 'USA'], $schema->names());
        static::assertFalse($schema->get('product')->isNullable());
        static::assertTrue($schema->get('USA')->isNullable());
        static::assertSame('float', $schema->get('USA')->type()->toString());
    }

    public function test_a_pivot_without_an_aggregation_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Pivot requires exactly one aggregation in group by, given: 0');

        new GroupBy(refs(ref('product')), new Aggregators(), new Pivot(ref('country'), pivot_values('USA')));
    }

    public function test_a_pivot_with_more_than_one_aggregation_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Pivot requires exactly one aggregation in group by, given: 2');

        new GroupBy(
            refs(ref('product')),
            new Aggregators(sum(ref('amount')), count(ref('amount'))),
            new Pivot(ref('country'), pivot_values('USA')),
        );
    }

    public function test_a_pivot_over_an_empty_input_yields_nothing(): void
    {
        static::assertSame(
            [],
            iterator_to_array((new PivotProcessor(
                new GroupBy(
                    refs(ref('product')),
                    new Aggregators(sum(ref('amount'))),
                    new Pivot(ref('country'), pivot_values('USA')),
                ),
                config()->grouping,
            ))->process((static fn(): Generator => yield from [])(), flow_context(config())->backend())),
        );
    }

    public function test_a_bound_pivot_over_an_empty_input_yields_nothing(): void
    {
        $group = new GroupBy(
            refs(ref('product')),
            new Aggregators(sum(ref('amount'))),
            new Pivot(ref('country'), pivot_values('USA')),
        );

        static::assertSame(
            [],
            iterator_to_array((new PivotProcessor(
                $group,
                config()->grouping,
                PivotShape::of($group, schema(str_schema('product'), str_schema('country'), int_schema('amount'))),
            ))->process((static fn(): Generator => yield from [])(), flow_context(config())->backend())),
        );
    }

    public function test_an_unbound_pivot_resolves_an_argument_typed_aggregate(): void
    {
        $result = iterator_to_array((new PivotProcessor(
            new GroupBy(
                refs(ref('product')),
                new Aggregators(min(ref('amount'))),
                new Pivot(ref('country'), pivot_values('USA')),
            ),
            config()->grouping,
        ))->process(
            (static function (): Generator {
                yield array_to_rows(
                    [
                        ['product' => 'Banana', 'country' => 'USA', 'amount' => 30],
                        ['product' => 'Banana', 'country' => 'USA', 'amount' => 10],
                    ],
                    schema(str_schema('product'), str_schema('country'), int_schema('amount')),
                );
            })(),
            flow_context(config())->backend(),
        ))[0];

        static::assertSame([['product' => 'Banana', 'USA' => 10]], $result->toArray());
        static::assertSame('integer', $result->schema()->get('USA')->type()->toString());
    }

    public function test_a_group_by_without_references_is_global(): void
    {
        static::assertTrue((new GroupBy(refs()))->isGlobal());
    }

    public function test_a_group_by_with_references_is_not_global(): void
    {
        static::assertFalse((new GroupBy(refs(ref('product'))))->isGlobal());
    }

    public function test_a_falsy_pivot_value_keeps_its_column(): void
    {
        $result = iterator_to_array((new PivotProcessor(
            new GroupBy(
                refs(ref('product')),
                new Aggregators(sum(ref('amount'))),
                new Pivot(ref('country'), pivot_values('0', 'USA')),
            ),
            config()->grouping,
        ))->process(
            (static function (): Generator {
                yield array_to_rows(
                    [
                        ['product' => 'Banana', 'country' => '0', 'amount' => 7],
                        ['product' => 'Banana', 'country' => 'USA', 'amount' => 3],
                    ],
                    schema(str_schema('product'), str_schema('country'), int_schema('amount')),
                );
            })(),
            flow_context(config())->backend(),
        ))[0];

        static::assertSame(['product', '0', 'USA'], $result->schema()->names());
        static::assertSame([['product' => 'Banana', '0' => 7.0, 'USA' => 3.0]], $result->toArray());
    }

    public function test_group_by_with_pivoting(): void
    {
        static::assertEquals(
            array_to_rows(
                [
                    ['product' => 'Banana', 'Canada' => 2000.0, 'China' => 400.0, 'Mexico' => null, 'USA' => 1000.0],
                    ['product' => 'Beans', 'Canada' => null, 'China' => 1500.0, 'Mexico' => 2000.0, 'USA' => 1600.0],
                    ['product' => 'Carrots', 'Canada' => 2000.0, 'China' => 1200.0, 'Mexico' => null, 'USA' => 1500.0],
                    ['product' => 'Orange', 'Canada' => null, 'China' => 4000.0, 'Mexico' => null, 'USA' => 4000.0],
                ],
                schema(
                    str_schema('product'),
                    // GroupBy::pivotSchema() declares every pivot column optional: the yield loop writes null
                    // for a combination it never saw, so nullability cannot depend on the data
                    float_schema('Canada', nullable: true),
                    float_schema('China', nullable: true),
                    float_schema('Mexico', nullable: true),
                    float_schema('USA', nullable: true),
                ),
            ),
            SortedRows::of(
                GroupByContext::aggregated(
                    new GroupBy(
                        refs(ref('product')),
                        new Aggregators(sum(ref('amount'))),
                        new Pivot(ref('country'), pivot_values('Canada', 'China', 'Mexico', 'USA')),
                    ),
                    flow_context(config()),
                    array_to_rows(
                        [
                            ['product' => 'Banana', 'amount' => 1000, 'country' => 'USA'],
                            ['product' => 'Carrots', 'amount' => 1500, 'country' => 'USA'],
                            ['product' => 'Beans', 'amount' => 1600, 'country' => 'USA'],
                            ['product' => 'Orange', 'amount' => 2000, 'country' => 'USA'],
                            ['product' => 'Orange', 'amount' => 2000, 'country' => 'USA'],
                            ['product' => 'Banana', 'amount' => 400, 'country' => 'China'],
                            ['product' => 'Carrots', 'amount' => 1200, 'country' => 'China'],
                            ['product' => 'Beans', 'amount' => 1500, 'country' => 'China'],
                            ['product' => 'Orange', 'amount' => 4000, 'country' => 'China'],
                            ['product' => 'Banana', 'amount' => 2000, 'country' => 'Canada'],
                            ['product' => 'Carrots', 'amount' => 2000, 'country' => 'Canada'],
                            ['product' => 'Beans', 'amount' => 2000, 'country' => 'Mexico'],
                        ],
                        schema(str_schema('product'), int_schema('amount'), str_schema('country')),
                    ),
                ),
                ref('product'),
            ),
        );
    }

    public function test_group_by_with_pivoting_with_null_pivot_column(): void
    {
        static::assertEquals(
            array_to_rows(
                [['product' => 'Apple', 'USA' => null], ['product' => 'Banana', 'USA' => 1000.0]],
                schema(str_schema('product'), float_schema('USA', nullable: true)),
            ),
            SortedRows::of(
                GroupByContext::aggregated(
                    new GroupBy(
                        refs(ref('product')),
                        new Aggregators(sum(ref('amount'))),
                        new Pivot(ref('country'), pivot_values('USA')),
                    ),
                    flow_context(config()),
                    array_to_rows(
                        [
                            ['product' => 'Banana', 'country' => 'USA', 'amount' => 1000],
                            ['product' => 'Apple', 'country' => null, 'amount' => 400],
                        ],
                        schema(str_schema('product'), str_schema('country', nullable: true), int_schema('amount')),
                    ),
                ),
                ref('product'),
            ),
        );
    }

    /**
     * Inventing a group column the batch schema does not declare would write a value into row storage
     * that no schema-driven reader can see, so the aggregate refuses at bind instead.
     */
    public function test_a_group_column_absent_from_the_schema_is_refused_at_bind(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);
        $this->expectExceptionMessage('Schema definition for column "missing" not found');

        GroupByContext::aggregated(
            new GroupBy(refs('missing'), new Aggregators(count())),
            flow_context(config()),
            array_to_rows([['type' => 'a'], ['type' => 'b']], schema(str_schema('type'))),
        );
    }

    public function test_a_not_null_group_key_stays_not_null_in_the_output_schema(): void
    {
        $groupBy = new GroupBy(refs('country'), new Aggregators(count()));

        $input = schema(str_schema('country'), int_schema('age'));

        static::assertEquals(
            schema(str_schema('country'), int_schema('_count')),
            $groupBy->outputSchema($input, $groupBy->aggregations()->resolved($input)),
        );
    }

    public function test_a_nullable_group_key_stays_nullable_in_the_output_schema(): void
    {
        $groupBy = new GroupBy(refs('country'), new Aggregators(count()));

        $input = schema(str_schema('country', nullable: true), int_schema('age'));

        static::assertEquals(
            schema(str_schema('country', nullable: true), int_schema('_count')),
            $groupBy->outputSchema($input, $groupBy->aggregations()->resolved($input)),
        );
    }

    public function test_keys_throws_when_a_row_lacks_a_not_null_key(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "country" does not exist.');

        GroupByContext::keys(
            new GroupBy(refs('country')),
            array_to_rows([['age' => 20]], schema(int_schema('age'))),
            schema(str_schema('country'), int_schema('age')),
        );
    }

    public function test_keys_substitutes_null_when_a_row_lacks_a_nullable_key(): void
    {
        static::assertSame(
            [['country' => null], ['country' => null]],
            array_map(
                static fn(GroupKey $key): array => iterator_to_array($key),
                GroupByContext::keys(
                    new GroupBy(refs('country')),
                    array_to_rows([['age' => 20], ['age' => 30]], schema(int_schema('age'))),
                    schema(str_schema('country', nullable: true), int_schema('age')),
                ),
            ),
        );
    }

    public function test_keys_reads_one_key_per_row(): void
    {
        static::assertSame(
            [['country' => 'PL', 'age' => 20], ['country' => 'DE', 'age' => 30]],
            array_map(
                static fn(GroupKey $key): array => iterator_to_array($key),
                GroupByContext::keys(
                    new GroupBy(refs('country', 'age')),
                    array_to_rows(
                        [['country' => 'PL', 'age' => 20], ['country' => 'DE', 'age' => 30]],
                        schema(str_schema('country'), int_schema('age')),
                    ),
                    schema(str_schema('country'), int_schema('age')),
                ),
            ),
        );
    }

    public function test_keys_of_an_empty_batch_are_empty(): void
    {
        static::assertSame(
            [],
            GroupByContext::keys(
                new GroupBy(refs('country')),
                rows(schema(int_schema('age'))),
                schema(str_schema('country')),
            ),
        );
    }

    public function test_equal_datetime_instants_in_other_zones_are_one_group(): void
    {
        $rows = array_to_rows([
            ['at' => new DateTimeImmutable('2026-01-02 10:00:00 UTC')],
            ['at' => new DateTimeImmutable('2026-01-02 11:00:00 +01:00')],
        ], schema(datetime_schema('at')));
        $keys = GroupByContext::keys(new GroupBy(refs('at')), $rows, $rows->schema());

        static::assertSame((string) $keys[0], (string) $keys[1]);
    }

    public function test_a_date_and_an_equal_datetime_instant_share_a_key(): void
    {
        $dates = array_to_rows([['at' => new DateTimeImmutable('2026-01-02')]], schema(date_schema('at')));
        $datetimes = array_to_rows([[
            'at' => new DateTimeImmutable('2026-01-02 00:00:00 UTC'),
        ]], schema(datetime_schema('at')));

        static::assertSame(
            (string) GroupByContext::keys(new GroupBy(refs('at')), $dates, $dates->schema())[0],
            (string) GroupByContext::keys(new GroupBy(refs('at')), $datetimes, $datetimes->schema())[0],
        );
    }

    public function test_uuid_and_json_keys_group_equal_values_and_keep_their_values(): void
    {
        $rows = array_to_rows(
            [
                ['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479', 'doc' => '{"a":1}'],
                ['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479', 'doc' => '{"a":1}'],
                ['id' => '00000000-0000-4000-8000-000000000000', 'doc' => '{"a":1}'],
            ],
            schema(uuid_schema('id'), json_schema('doc')),
        );
        $keys = GroupByContext::keys(new GroupBy(refs('id', 'doc')), $rows, $rows->schema());

        static::assertSame((string) $keys[0], (string) $keys[1]);
        static::assertNotSame((string) $keys[0], (string) $keys[2]);
        static::assertEquals(
            ['id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 'doc' => new Json('{"a":1}')],
            iterator_to_array($keys[0]),
        );
    }

    public function test_keys_of_a_batch_that_lacks_a_nullable_key_match_a_present_null(): void
    {
        $input = schema(str_schema('a'), str_schema('b', nullable: true));
        $lacking = GroupByContext::keys(
            new GroupBy(refs('a', 'b')),
            array_to_rows([['a' => 'x']], schema(str_schema('a'))),
            $input,
        );
        $present = GroupByContext::keys(
            new GroupBy(refs('a', 'b')),
            array_to_rows([['a' => 'x', 'b' => null]], $input),
            $input,
        );

        static::assertSame(['a' => 'x', 'b' => null], iterator_to_array($lacking[0]));
        static::assertSame((string) $present[0], (string) $lacking[0]);
    }

    public function test_keys_of_swapped_absences_do_not_collide(): void
    {
        $input = schema(str_schema('a', nullable: true), str_schema('b', nullable: true));
        static::assertNotSame(
            (string) GroupByContext::keys(
                new GroupBy(refs('a', 'b')),
                array_to_rows([['a' => 'x']], schema(str_schema('a'))),
                $input,
            )[0],
            (string) GroupByContext::keys(
                new GroupBy(refs('a', 'b')),
                array_to_rows([['b' => 'x']], schema(str_schema('b'))),
                $input,
            )[0],
        );
    }

    public function test_zero_and_negative_zero_are_one_key(): void
    {
        $rows = array_to_rows([['v' => 0.0], ['v' => -0.0], ['v' => 1.5], ['v' => 1.5]], schema(float_schema('v')));

        static::assertCount(
            2,
            array_unique(array_map(
                static fn(GroupKey $key): string => (string) $key,
                GroupByContext::keys(new GroupBy(refs('v')), $rows, $rows->schema()),
            )),
        );
    }

    public function test_pivot_schema_keeps_the_group_by_column_declarations_of_the_input(): void
    {
        static::assertEquals(
            schema(str_schema('product'), float_schema('USA', nullable: true)),
            (new GroupBy(refs('product')))->pivotSchema(
                schema(str_schema('product'), str_schema('country'), int_schema('amount')),
                ['USA'],
                sum(ref('amount')),
            ),
        );
    }

    public function test_pivot_schema_types_every_pivot_column_from_the_aggregate_as_nullable(): void
    {
        $output = (new GroupBy(refs('product')))->pivotSchema(
            schema(str_schema('product'), str_schema('country'), int_schema('amount')),
            ['USA', 'Canada'],
            sum(ref('amount')),
        );

        foreach (['USA', 'Canada'] as $column) {
            static::assertTrue($output->get($column)->isNullable(), $column);
            static::assertSame('float', $output->get($column)->type()->toString(), $column);
        }
    }

    public function test_pivot_schema_names_an_integer_pivot_column_by_its_digits(): void
    {
        static::assertSame(
            ['product', '0', '7'],
            (new GroupBy(refs('product')))
                ->pivotSchema(
                    schema(str_schema('product'), int_schema('year'), int_schema('amount')),
                    [0, 7],
                    sum(ref('amount')),
                )
                ->names(),
        );
    }

    public function test_pivot_schema_refuses_a_pivot_value_colliding_with_a_group_by_column(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Pivot value "product" collides with the group-by column of the same name');

        (new GroupBy(refs('product')))->pivotSchema(
            schema(str_schema('product'), int_schema('amount')),
            ['product'],
            sum(ref('amount')),
        );
    }

    public function test_pivot_schema_without_values_declares_only_the_group_by_columns(): void
    {
        static::assertEquals(
            schema(str_schema('product')),
            (new GroupBy(refs('product')))->pivotSchema(
                schema(str_schema('product'), int_schema('amount')),
                [],
                sum(ref('amount')),
            ),
        );
    }

    public function test_read_columns_are_the_grouping_columns_then_the_aggregated_ones(): void
    {
        static::assertSame(
            ['category', 'amount', 'qty'],
            (new GroupBy(refs(ref('category')), new Aggregators(sum(ref('amount')), min(ref('qty')))))->readColumns(),
        );
    }

    public function test_read_columns_name_a_column_read_twice_once(): void
    {
        static::assertSame(
            ['category', 'amount'],
            (new GroupBy(
                refs(ref('category')),
                new Aggregators(sum(ref('amount')), min(ref('amount')), count(ref('category'))),
            ))->readColumns(),
        );
    }

    public function test_read_columns_are_null_when_an_aggregation_names_no_columns(): void
    {
        static::assertNull(
            (new GroupBy(refs(ref('category')), new Aggregators(new UnenumerableSum(ref('amount')))))->readColumns(),
        );
    }
}
