<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use DateTimeImmutable;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\GroupKey;
use Flow\ETL\GroupBy\PivotAggregation;
use Flow\ETL\GroupBy\PivotShape;
use Flow\ETL\Tests\Context\GroupByContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use Generator;

use function array_map;
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
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\uuid_schema;
use function iterator_to_array;

final class GroupByTest extends FlowTestCase
{
    public function test_group_by_missing_entry(): void
    {
        $groupBy = new GroupBy('type');
        $groupBy->aggregate(count());

        $result = GroupByContext::aggregated(
            $groupBy,
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
        $group = new GroupBy('type');
        $group->aggregate(sum(ref('id')));

        $result = GroupByContext::aggregated(
            $group,
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
        $group->pivot(ref('country'), pivot_values('USA'));

        $schema = iterator_to_array((new PivotAggregation())->aggregate(
            (static function (): Generator {
                yield array_to_rows(
                    [['product' => 'Banana', 'country' => 'USA', 'amount' => 1000]],
                    schema(str_schema('product'), str_schema('country'), int_schema('amount')),
                );
            })(),
            flow_context(config()),
            $group,
        ))[0]->schema();

        static::assertSame(['product', 'USA'], $schema->references()->names());
        static::assertFalse($schema->get('product')->isNullable());
        static::assertTrue($schema->get('USA')->isNullable());
        static::assertSame('float', $schema->get('USA')->type()->toString());
    }

    public function test_a_pivot_without_an_aggregation_is_refused(): void
    {
        $group = new GroupBy(ref('product'));
        $group->pivot(ref('country'), pivot_values('USA'));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Pivot requires exactly one aggregation');

        iterator_to_array((new PivotAggregation())->aggregate(
            (static fn(): Generator => yield from [])(),
            flow_context(config()),
            $group,
        ));
    }

    public function test_a_pivot_with_more_than_one_aggregation_is_refused(): void
    {
        $group = new GroupBy(ref('product'));
        $group->pivot(ref('country'), pivot_values('USA'));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Pivot requires exactly one aggregation in group by, given: 2');

        $group->aggregate(sum(ref('amount')), count(ref('amount')));
    }

    public function test_a_pivot_over_an_empty_input_yields_nothing(): void
    {
        $group = new GroupBy(ref('product'));
        $group->aggregate(sum(ref('amount')));
        $group->pivot(ref('country'), pivot_values('USA'));

        static::assertSame(
            [],
            iterator_to_array((new PivotAggregation())->aggregate(
                (static fn(): Generator => yield from [])(),
                flow_context(config()),
                $group,
            )),
        );
    }

    public function test_a_bound_pivot_over_an_empty_input_yields_nothing(): void
    {
        $group = new GroupBy(ref('product'));
        $group->aggregate(sum(ref('amount')));
        $group->pivot(ref('country'), pivot_values('USA'));

        static::assertSame(
            [],
            iterator_to_array((new PivotAggregation())->aggregateBound(
                (static fn(): Generator => yield from [])(),
                flow_context(config()),
                $group,
                PivotShape::of($group, schema(str_schema('product'), str_schema('country'), int_schema('amount'))),
            )),
        );
    }

    public function test_an_unbound_pivot_resolves_an_argument_typed_aggregate(): void
    {
        $group = new GroupBy(ref('product'));
        $group->aggregate(min(ref('amount')));
        $group->pivot(ref('country'), pivot_values('USA'));

        $result = iterator_to_array((new PivotAggregation())->aggregate(
            (static function (): Generator {
                yield array_to_rows(
                    [
                        ['product' => 'Banana', 'country' => 'USA', 'amount' => 30],
                        ['product' => 'Banana', 'country' => 'USA', 'amount' => 10],
                    ],
                    schema(str_schema('product'), str_schema('country'), int_schema('amount')),
                );
            })(),
            flow_context(config()),
            $group,
        ))[0];

        static::assertSame([['product' => 'Banana', 'USA' => 10]], $result->toArray());
        static::assertSame('integer', $result->schema()->get('USA')->type()->toString());
    }

    public function test_a_group_by_without_references_is_global(): void
    {
        static::assertTrue((new GroupBy())->isGlobal());
    }

    public function test_a_group_by_with_references_is_not_global(): void
    {
        static::assertFalse((new GroupBy(ref('product')))->isGlobal());
    }

    public function test_a_falsy_pivot_value_keeps_its_column(): void
    {
        $group = new GroupBy(ref('product'));
        $group->aggregate(sum(ref('amount')));
        $group->pivot(ref('country'), pivot_values('0', 'USA'));

        $result = iterator_to_array((new PivotAggregation())->aggregate(
            (static function (): Generator {
                yield array_to_rows(
                    [
                        ['product' => 'Banana', 'country' => '0', 'amount' => 7],
                        ['product' => 'Banana', 'country' => 'USA', 'amount' => 3],
                    ],
                    schema(str_schema('product'), str_schema('country'), int_schema('amount')),
                );
            })(),
            flow_context(config()),
            $group,
        ))[0];

        static::assertSame(['product', '0', 'USA'], $result->schema()->references()->names());
        static::assertSame([['product' => 'Banana', '0' => 7.0, 'USA' => 3.0]], $result->toArray());
    }

    public function test_group_by_with_pivoting(): void
    {
        $group = new GroupBy(ref('product'));
        $group->aggregate(sum(ref('amount')));
        $group->pivot(ref('country'), pivot_values('Canada', 'China', 'Mexico', 'USA'));

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
                    // PivotSchema declares every pivot column optional: the yield loop writes null
                    // for a combination it never saw, so nullability cannot depend on the data
                    float_schema('Canada', nullable: true),
                    float_schema('China', nullable: true),
                    float_schema('Mexico', nullable: true),
                    float_schema('USA', nullable: true),
                ),
            ),
            GroupByContext::aggregated(
                $group,
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
            )->sortBy(ref('product')),
        );
    }

    public function test_group_by_with_pivoting_with_null_pivot_column(): void
    {
        $group = new GroupBy(ref('product'));
        $group->aggregate(sum(ref('amount')));
        $group->pivot(ref('country'), pivot_values('USA'));

        static::assertEquals(
            array_to_rows(
                [['product' => 'Apple', 'USA' => null], ['product' => 'Banana', 'USA' => 1000.0]],
                schema(str_schema('product'), float_schema('USA', nullable: true)),
            ),
            GroupByContext::aggregated(
                $group,
                flow_context(config()),
                array_to_rows(
                    [
                        ['product' => 'Banana', 'country' => 'USA', 'amount' => 1000],
                        ['product' => 'Apple', 'country' => null, 'amount' => 400],
                    ],
                    schema(str_schema('product'), str_schema('country', nullable: true), int_schema('amount')),
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
            array_to_rows([['type' => 'a'], ['type' => 'b']], schema(str_schema('type'))),
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

    public function test_keys_throws_when_a_row_lacks_a_not_null_key(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "country" does not exist.');

        (new GroupBy('country'))->keys(
            array_to_rows([['age' => 20]], schema(int_schema('age'))),
            schema(str_schema('country'), int_schema('age')),
        );
    }

    public function test_keys_substitutes_null_when_a_row_lacks_a_nullable_key(): void
    {
        $keys = (new GroupBy('country'))->keys(
            array_to_rows([['age' => 20], ['age' => 30]], schema(int_schema('age'))),
            schema(str_schema('country', nullable: true), int_schema('age')),
        );

        static::assertSame(
            [['country' => null], ['country' => null]],
            array_map(static fn(GroupKey $key): array => iterator_to_array($key), $keys),
        );
    }

    public function test_keys_reads_one_key_per_row(): void
    {
        $keys = (new GroupBy('country', 'age'))->keys(
            array_to_rows(
                [['country' => 'PL', 'age' => 20], ['country' => 'DE', 'age' => 30]],
                schema(str_schema('country'), int_schema('age')),
            ),
            schema(str_schema('country'), int_schema('age')),
        );

        static::assertSame(
            [['country' => 'PL', 'age' => 20], ['country' => 'DE', 'age' => 30]],
            array_map(static fn(GroupKey $key): array => iterator_to_array($key), $keys),
        );
    }

    public function test_keys_of_an_empty_batch_are_empty(): void
    {
        static::assertSame(
            [],
            (new GroupBy('country'))->keys(rows(schema(int_schema('age'))), schema(str_schema('country'))),
        );
    }

    public function test_equal_datetime_instants_in_other_zones_are_one_group(): void
    {
        $rows = array_to_rows([
            ['at' => new DateTimeImmutable('2026-01-02 10:00:00 UTC')],
            ['at' => new DateTimeImmutable('2026-01-02 11:00:00 +01:00')],
        ], schema(datetime_schema('at')));
        $keys = (new GroupBy('at'))->keys($rows, $rows->schema());

        static::assertSame((string) $keys[0], (string) $keys[1]);
    }

    public function test_a_date_and_an_equal_datetime_instant_share_a_key(): void
    {
        $dates = array_to_rows([['at' => new DateTimeImmutable('2026-01-02')]], schema(date_schema('at')));
        $datetimes = array_to_rows([[
            'at' => new DateTimeImmutable('2026-01-02 00:00:00 UTC'),
        ]], schema(datetime_schema('at')));

        static::assertSame(
            (string) (new GroupBy('at'))->keys($dates, $dates->schema())[0],
            (string) (new GroupBy('at'))->keys($datetimes, $datetimes->schema())[0],
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
        $keys = (new GroupBy('id', 'doc'))->keys($rows, $rows->schema());

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
        $lacking = (new GroupBy('a', 'b'))->keys(array_to_rows([['a' => 'x']], schema(str_schema('a'))), $input);
        $present = (new GroupBy('a', 'b'))->keys(array_to_rows([['a' => 'x', 'b' => null]], $input), $input);

        static::assertSame(['a' => 'x', 'b' => null], iterator_to_array($lacking[0]));
        static::assertSame((string) $present[0], (string) $lacking[0]);
    }

    public function test_keys_of_swapped_absences_do_not_collide(): void
    {
        $input = schema(str_schema('a', nullable: true), str_schema('b', nullable: true));
        $lacksB = (new GroupBy('a', 'b'))->keys(array_to_rows([['a' => 'x']], schema(str_schema('a'))), $input);
        $lacksA = (new GroupBy('a', 'b'))->keys(array_to_rows([['b' => 'x']], schema(str_schema('b'))), $input);

        static::assertNotSame((string) $lacksB[0], (string) $lacksA[0]);
    }
}
