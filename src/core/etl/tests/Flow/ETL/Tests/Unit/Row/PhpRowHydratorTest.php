<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use DateTimeImmutable;
use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\Double\ForeignTypeDefinition;
use Flow\ETL\Tests\Double\ThrowingType;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Value\Uuid;
use Generator;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_positive_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class PhpRowHydratorTest extends FlowTestCase
{
    /**
     * @return \Generator<string, array{Schema, list<RawRowValues>, class-string<\Throwable>, string}>
     */
    public static function refusing_datasets(): Generator
    {
        yield 'null in a not null column' => [
            schema(int_schema('id'), str_schema('name')),
            [new RawRowValues(['id' => 1, 'name' => null])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "name" (row 0): could not convert null to string, column is not nullable',
        ];

        yield 'non numeric string in an integer column' => [
            schema(int_schema('id')),
            [new RawRowValues(['id' => 'abc'])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "id" (row 0): could not convert \'abc\' (string) to integer',
        ];

        yield 'trailing garbage after digits in an integer column' => [
            schema(int_schema('id')),
            [new RawRowValues(['id' => '12abc'])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "id" (row 0): could not convert \'12abc\' (string) to integer',
        ];

        yield 'hex string in a float column' => [
            schema(float_schema('price')),
            [new RawRowValues(['price' => '0x1A'])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "price" (row 0): could not convert \'0x1A\' (string) to float',
        ];

        yield 'unrecognised word in a boolean column' => [
            schema(bool_schema('active')),
            [new RawRowValues(['active' => 'weird'])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "active" (row 0): could not convert \'weird\' (string) to boolean',
        ];

        yield 'array in an integer column' => [
            schema(int_schema('id')),
            [new RawRowValues(['id' => [1, 2, 3]])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "id" (row 0): could not convert array (list<integer>) to integer',
        ];

        yield 'invalid uuid string' => [
            schema(uuid_schema('u')),
            [new RawRowValues(['u' => 'not-a-uuid'])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "u" (row 0): could not convert \'not-a-uuid\' (string) to uuid',
        ];

        yield 'uppercase uuid string' => [
            schema(uuid_schema('u')),
            [new RawRowValues(['u' => '01234567-89AB-4DEF-8123-456789ABCDEF'])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "u" (row 0): could not convert \'01234567-89AB-4DEF-8123-456789AB...\' (string) to uuid',
        ];

        yield 'json from a scalar' => [
            schema(json_schema('j')),
            [new RawRowValues(['j' => 5])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "j" (row 0): could not convert 5 (integer) to json',
        ];

        yield 'json from an invalid json string' => [
            schema(json_schema('j')),
            [new RawRowValues(['j' => '{oops'])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "j" (row 0): could not convert \'{oops\' (string) to json',
        ];

        yield 'json from a plain string' => [
            schema(json_schema('j')),
            [new RawRowValues(['j' => 'plain'])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "j" (row 0): could not convert \'plain\' (string) to json',
        ];

        yield 'datetime from garbage' => [
            schema(datetime_schema('at')),
            [new RawRowValues(['at' => 'not-a-date'])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "at" (row 0): could not convert \'not-a-date\' (string) to datetime',
        ];

        yield 'datetime from an array' => [
            schema(datetime_schema('at')),
            [new RawRowValues(['at' => ['nope']])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "at" (row 0): could not convert array (list<string>) to datetime',
        ];

        yield 'date from garbage' => [
            schema(date_schema('d')),
            [new RawRowValues(['d' => 'not-a-date'])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "d" (row 0): could not convert \'not-a-date\' (string) to date',
        ];

        yield 'string map with integer keys' => [
            schema(map_schema('m', type_map(type_string(), type_integer()))),
            [new RawRowValues(['m' => [5 => 1]])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "m" (row 0): could not convert array (map<integer, integer>) to map<string, integer>',
        ];

        yield 'list with non-sequential keys' => [
            schema(list_schema('l', type_list(type_integer()))),
            [new RawRowValues(['l' => [1 => 'x']])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "l" (row 0): could not convert array (map<integer, string>) to list<integer>',
        ];

        yield 'positive integer list from a non-numeric string' => [
            schema(list_schema('l', type_list(type_positive_integer()))),
            [new RawRowValues(['l' => ['abc']])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "l" (row 0): could not convert array (list<string>) to list<positive_integer>',
        ];

        yield 'positive integer list from a negative int' => [
            schema(list_schema('l', type_list(type_positive_integer()))),
            [new RawRowValues(['l' => [-3]])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "l" (row 0): could not convert array (list<integer>) to list<positive_integer>',
        ];

        yield 'structure missing required element' => [
            schema(structure_schema('data', type_structure(['id' => type_integer(), 'name' => type_string()]))),
            [new RawRowValues(['data' => ['id' => 1]])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "data" (row 0): could not convert array (structure{id: integer}) to structure{id: integer, name: string}',
        ];

        yield 'structure present-null required element' => [
            schema(structure_schema('data', type_structure(['id' => type_integer(), 'name' => type_string()]))),
            [new RawRowValues(['data' => ['id' => 1, 'name' => null]])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "data" (row 0): could not convert array (structure{id: integer, name: null}) to structure{id: integer, name: string}',
        ];

        yield 'structure present-null optional element' => [
            schema(structure_schema('data', type_structure([
                'id' => type_integer(),
                'name' => structure_element('name', type_string(), optional: true),
            ]))),
            [new RawRowValues(['data' => ['id' => 1, 'name' => null]])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "data" (row 0): could not convert array (structure{id: integer, name: null}) to structure{id: integer, name?: string}',
        ];

        // the refusal is placed at the batch position, so a constant row index would diverge here
        yield 'non numeric string in the second row of an integer column' => [
            schema(int_schema('id')),
            [new RawRowValues(['id' => '1']), new RawRowValues(['id' => 'x'])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "id" (row 1): could not convert \'x\' (string) to integer',
        ];

        yield 'a not null column the row does not carry' => [
            schema(int_schema('id'), str_schema('name')),
            [new RawRowValues(['id' => 1])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "name" (row 0) declared by the schema is missing from the row',
        ];

        yield 'integer overflow' => [
            schema(int_schema('id')),
            [new RawRowValues(['id' => '9223372036854775808'])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "id" (row 0): could not convert \'9223372036854775808\' (string) to integer',
        ];

        yield 'date from an empty string' => [
            schema(date_schema('d')),
            [new RawRowValues(['d' => ''])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "d" (row 0): could not convert \'\' (string) to date',
        ];

        yield 'datetime from a relative word' => [
            schema(datetime_schema('at')),
            [new RawRowValues(['at' => 'now'])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "at" (row 0): could not convert \'now\' (string) to datetime',
        ];

        yield 'string from a null element inside a list' => [
            schema(list_schema('l', type_list(type_string()))),
            [new RawRowValues(['l' => ['a', null]])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "l" (row 0): could not convert array (list<?string>) to list<string>',
        ];

        yield 'list from a scalar' => [
            schema(list_schema('l', type_list(type_integer()))),
            [new RawRowValues(['l' => 5])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "l" (row 0): could not convert 5 (integer) to list<integer>',
        ];

        // ordering: an absence must not pre-empt a refusal that PHP reports first, or the two
        // hydrators name different columns - and different ROWS - for the same batch
        yield 'an absent column before a cast refusal in the same row' => [
            schema(int_schema('a'), int_schema('b')),
            [new RawRowValues(['b' => 'abc'])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "b" (row 0): could not convert \'abc\' (string) to integer',
        ];

        yield 'an absent column in row 0 before a cast refusal in row 1' => [
            schema(int_schema('a'), int_schema('b')),
            [new RawRowValues(['a' => 1]), new RawRowValues(['a' => 1, 'b' => 'abc'])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "b" (row 1): could not convert \'abc\' (string) to integer',
        ];

        yield 'an absent column in row 0 before a present null in row 1' => [
            schema(int_schema('a'), int_schema('b')),
            [new RawRowValues(['a' => 1]), new RawRowValues(['a' => 1, 'b' => null])],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "b" (row 1): could not convert null to integer, column is not nullable',
        ];
    }

    /**
     * @param list<RawRowValues> $batch
     * @param class-string<\Throwable> $exception
     */
    #[DataProvider('refusing_datasets')]
    public function test_refuses(Schema $schema, array $batch, string $exception, string $message): void
    {
        $this->expectException($exception);
        $this->expectExceptionMessage($message);

        (new PhpRowHydrator())->hydrate($batch, $schema);
    }

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

    public function test_refuses_a_definition_outside_the_nineteen(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage(
            'Row does not match its schema: column "a": throwing cannot be a batch column, only the 19 Flow definitions have a column kind',
        );

        (new PhpRowHydrator())->hydrate([new RawRowValues(['a' => [
            1,
            2,
        ]])], schema(new ForeignTypeDefinition('a', new ThrowingType(new LogicException('stub type refuses everything')))));
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

    public function test_metadata_is_folded_onto_the_column_with_last_write_winning(): void
    {
        static::assertSame(
            ['k' => 'v2'],
            (new PhpRowHydrator())
                ->hydrate([
                    new RawRowValues(['id' => 1], ['id' => Metadata::fromArray(['k' => 'v1'])]),
                    new RawRowValues(['id' => 2], ['id' => Metadata::fromArray(['k' => 'v2'])]),
                ], schema(int_schema('id')))
                ->schema()
                ->get('id')
                ->metadata()
                ->normalize(),
        );
    }

    public function test_metadata_is_folded_onto_a_numeric_column_name(): void
    {
        static::assertSame(
            ['k' => 'v'],
            (new PhpRowHydrator())
                ->hydrate([new RawRowValues([], ['0' => Metadata::fromArray([
                    'k' => 'v',
                ])])], schema(int_schema('0', nullable: true)))
                ->schema()
                ->get('0')
                ->metadata()
                ->normalize(),
        );
    }

    public function test_metadata_for_an_undeclared_column_is_ignored(): void
    {
        static::assertEquals(
            schema(int_schema('id')),
            (new PhpRowHydrator())
                ->hydrate([new RawRowValues(['id' => 1], ['nope' => Metadata::fromArray([
                    'k' => 'v',
                ])])], schema(int_schema('id')))
                ->schema(),
        );
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
        static::assertEquals($createdAt, $rows->first()->get('created_at'));
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

        static::assertEquals($rows, $hydrator->hydrate([new RawRowValues($rows->first()->values())], $schema));
    }
}
