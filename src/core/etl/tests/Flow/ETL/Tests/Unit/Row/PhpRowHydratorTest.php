<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use DateTimeImmutable;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Schema\Definition\UnionDefinition;
use Flow\ETL\Tests\Double\ThrowingType;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Value\Uuid;
use LogicException;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_union;
use function serialize;

final class PhpRowHydratorTest extends FlowTestCase
{
    public function test_absent_schema_column_is_filled_with_typed_null(): void
    {
        $rows = (new PhpRowHydrator())->hydrate(
            [new RawRowValues(['id' => '1'])],
            schema(int_schema('id'), str_schema('name', nullable: true)),
        );

        static::assertTrue($rows->first()->has('name'));
        static::assertNull($rows->first()->get('name'));
        static::assertTrue($rows->schema()->get('name')->isNullable());
        static::assertSame(['id' => 1, 'name' => null], $rows->first()->toArray());
    }

    public function test_hydrate_does_not_wrap_an_exception_the_types_package_did_not_raise(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('stub type refuses everything');

        (new PhpRowHydrator())->hydrate([new RawRowValues(['a' => [
            1,
            2,
        ]])], schema(new UnionDefinition('a', type_union(new ThrowingType(new LogicException('stub type refuses everything')), type_string()))));
    }

    public function test_hydrate_throws_on_missing_required_structure_element(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('Rows do not match their schema: column "data" (row 0)');

        (new PhpRowHydrator())->hydrate([new RawRowValues(['data' => [
            'id' => 1,
        ]])], schema(structure_schema('data', type_structure(['id' => type_integer(), 'name' => type_string()]))));
    }

    public function test_casts_datetime_and_uuid_strings(): void
    {
        $rows = (new PhpRowHydrator())->hydrate(
            [new RawRowValues([
                'created_at' => '2024-01-01 12:00:00 UTC',
                'uuid' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479',
            ])],
            schema(datetime_schema('created_at'), uuid_schema('uuid')),
        );

        $uuid = $rows->first()->get('uuid');

        static::assertInstanceOf(DateTimeImmutable::class, $rows->first()->get('created_at'));
        static::assertInstanceOf(Uuid::class, $uuid);
        static::assertSame('f47ac10b-58cc-4372-a567-0e02b2c3d479', $uuid->toString());
    }

    public function test_casts_raw_scalar_strings_to_schema_types(): void
    {
        $rows = (new PhpRowHydrator())->hydrate(
            [new RawRowValues(['id' => '1', 'price' => '9.99', 'active' => 'true', 'name' => 'Alice'])],
            schema(int_schema('id'), float_schema('price'), bool_schema('active'), str_schema('name')),
        );

        static::assertSame(
            ['id' => 1, 'price' => 9.99, 'active' => true, 'name' => 'Alice'],
            $rows->first()->toArray(),
        );
    }

    public function test_columns_absent_from_the_schema_are_dropped(): void
    {
        $rows = (new PhpRowHydrator())->hydrate([new RawRowValues([
            'id' => '1',
            'extra' => 'raw',
        ])], schema(int_schema('id')));

        static::assertFalse($rows->first()->has('extra'));
        static::assertSame(['id' => 1], $rows->first()->toArray());
    }

    public function test_dehydrate_returns_typed_values_keyed_by_entry_name(): void
    {
        $hydrator = new PhpRowHydrator();
        $schema = schema(int_schema('id'), str_schema('name', nullable: true));

        $batch = [
            new RawRowValues(['id' => 1, 'name' => 'Alice']),
            new RawRowValues(['id' => 2, 'name' => null]),
        ];

        static::assertEquals(
            [
                new TypedRowValues(['id' => 1, 'name' => 'Alice'], ['id' => type_integer(), 'name' => type_string()]),
                new TypedRowValues(['id' => 2, 'name' => null], ['id' => type_integer(), 'name' => type_string()]),
            ],
            $hydrator->dehydrate($hydrator->hydrate($batch, $schema)),
        );
    }

    public function test_empty_batch_produces_empty_rows(): void
    {
        static::assertCount(0, (new PhpRowHydrator())->hydrate([], schema(int_schema('id'))));
    }

    public function test_hydrate_keeps_native_typed_values(): void
    {
        $rows = (new PhpRowHydrator())->hydrate(
            [
                new RawRowValues(['id' => 1, 'name' => 'Alice']),
                new RawRowValues(['id' => 2, 'name' => 'Bob']),
            ],
            schema(int_schema('id'), str_schema('name')),
        );

        static::assertSame(
            [
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
            ],
            $rows->toArray(),
        );
    }

    public function test_hydrate_keeps_native_uuid_value_objects(): void
    {
        $rows = (new PhpRowHydrator())->hydrate([new RawRowValues([
            'id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479'),
        ])], schema(uuid_schema('id')));

        $value = $rows->first()->get('id');

        static::assertInstanceOf(Uuid::class, $value);
        static::assertSame('f47ac10b-58cc-4372-a567-0e02b2c3d479', $value->toString());
    }

    public function test_null_value_in_a_non_nullable_column_is_refused(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('Rows do not match their schema: column "name" (row 0): could not convert null to string, '
        . 'column is not nullable');

        (new PhpRowHydrator())->hydrate(
            [new RawRowValues(['id' => 1, 'name' => null])],
            schema(int_schema('id'), str_schema('name')),
        );
    }

    public function test_null_value_keeps_the_schema_definition(): void
    {
        $rows = (new PhpRowHydrator())->hydrate(
            [new RawRowValues(['id' => 1, 'name' => null])],
            schema(int_schema('id'), str_schema('name', nullable: true)),
        );

        static::assertNull($rows->first()->get('name'));
        static::assertTrue($rows->schema()->get('name')->isNullable());
    }

    public function test_plan_is_rebuilt_when_the_schema_changes(): void
    {
        $hydrator = new PhpRowHydrator();

        $ids = $hydrator->hydrate([new RawRowValues(['id' => '1'])], schema(int_schema('id')));
        $prices = $hydrator->hydrate([new RawRowValues(['id' => '1'])], schema(float_schema('id')));

        static::assertSame(['id' => 1], $ids->first()->toArray());
        static::assertSame(['id' => 1.0], $prices->first()->toArray());
    }

    public function test_rows_share_the_declared_schema_instance(): void
    {
        $schema = schema(int_schema('id'));

        $rows = (new PhpRowHydrator())->hydrate([
            new RawRowValues(['id' => 1]),
            new RawRowValues(['id' => 2]),
        ], $schema);

        static::assertSame($schema, $rows->schema());
    }

    public function test_hydrate_does_not_cast_native_values(): void
    {
        $createdAt = new DateTimeImmutable('2024-01-01 12:00:00 UTC');

        $rows = (new PhpRowHydrator())->hydrate(
            [new RawRowValues(['id' => 1, 'created_at' => $createdAt])],
            schema(int_schema('id'), datetime_schema('created_at')),
        );

        static::assertSame(1, $rows->first()->get('id'));
        static::assertSame($createdAt, $rows->first()->get('created_at'));
    }

    public function test_hydrate_does_not_cast_nested_list_map_and_structure(): void
    {
        $rows = (new PhpRowHydrator())->hydrate(
            [new RawRowValues([
                'tags' => ['a', 'b'],
                'counts' => ['x' => 1, 'y' => 2],
                'address' => ['city' => 'NYC', 'zip' => 10001],
            ])],
            schema(
                list_schema('tags', type_list(type_string())),
                map_schema('counts', type_map(type_string(), type_integer())),
                structure_schema('address', type_structure(['city' => type_string(), 'zip' => type_integer()])),
            ),
        );

        static::assertInstanceOf(ListType::class, $rows->schema()->get('tags')->type());
        static::assertInstanceOf(MapType::class, $rows->schema()->get('counts')->type());
        static::assertInstanceOf(StructureType::class, $rows->schema()->get('address')->type());
        static::assertSame(['a', 'b'], $rows->first()->get('tags'));
        static::assertSame(['x' => 1, 'y' => 2], $rows->first()->get('counts'));
        static::assertSame(['city' => 'NYC', 'zip' => 10001], $rows->first()->get('address'));
    }

    public function test_hydrate_pads_a_nullable_column_absent_from_the_values(): void
    {
        $rows = (new PhpRowHydrator())->hydrate(
            [new RawRowValues(['id' => 1])],
            schema(int_schema('id'), str_schema('name', nullable: true)),
        );

        static::assertTrue($rows->first()->has('id'));
        static::assertTrue($rows->first()->has('name'));
        static::assertNull($rows->first()->get('name'));
    }

    public function test_hydrating_already_typed_values_is_idempotent(): void
    {
        $schema = schema(int_schema('id'), str_schema('name'), datetime_schema('created_at'));
        $hydrator = new PhpRowHydrator();
        $rows = $hydrator->hydrate([new RawRowValues([
            'id' => 1,
            'name' => 'Alice',
            'created_at' => new DateTimeImmutable('2024-01-01 00:00:00 UTC'),
        ])], $schema);

        static::assertSame(
            serialize($rows),
            serialize($hydrator->hydrate([new RawRowValues($rows->first()->values())], $schema)),
        );
    }
}
