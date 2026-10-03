<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Rows;

use DateTimeImmutable;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowsBuilder;
use Flow\ETL\Schema;
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

use function Flow\ETL\DSL\array_to_rows;
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

final class RowsBuilderTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{Rows, Schema, string}>
     */
    public static function refused_views(): Generator
    {
        yield 'a not null column the source lacks' => [
            array_to_rows([['id' => 1]], schema(int_schema('id'))),
            schema(int_schema('id'), str_schema('name')),
            'column "name" (row 0) declared by the schema is missing from the row',
        ];
        yield 'null under not null' => [
            array_to_rows([['id' => null]], schema(int_schema('id', nullable: true))),
            schema(int_schema('id')),
            'column "id" (row 0): could not convert null to integer, column is not nullable',
        ];
        yield 'a value that matches only after a cast' => [
            array_to_rows([['id' => '2']], schema(str_schema('id'))),
            schema(int_schema('id')),
            'column "id" (row 0): could not convert \'2\' (string) to integer',
        ];
        yield 'a column the target does not declare' => [
            array_to_rows([['id' => 1, 'extra' => 2]], schema(int_schema('id'), int_schema('extra'))),
            schema(int_schema('id')),
            'column "extra" (row 0) is not declared by the schema',
        ];
    }

    /**
     * @return \Generator<string, array{Schema, list<array<array-key, mixed>>, class-string<\Throwable>, string}>
     */
    public static function refused_rows(): Generator
    {
        yield 'null in a not null column' => [
            schema(int_schema('id'), str_schema('name')),
            [['id' => 1, 'name' => null]],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "name" (row 0): could not convert null to string, column is not nullable',
        ];

        yield 'non numeric string in an integer column' => [
            schema(int_schema('id')),
            [['id' => 'abc']],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "id" (row 0): could not convert \'abc\' (string) to integer',
        ];

        yield 'trailing garbage after digits in an integer column' => [
            schema(int_schema('id')),
            [['id' => '12abc']],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "id" (row 0): could not convert \'12abc\' (string) to integer',
        ];

        yield 'hex string in a float column' => [
            schema(float_schema('price')),
            [['price' => '0x1A']],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "price" (row 0): could not convert \'0x1A\' (string) to float',
        ];

        yield 'unrecognised word in a boolean column' => [
            schema(bool_schema('active')),
            [['active' => 'weird']],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "active" (row 0): could not convert \'weird\' (string) to boolean',
        ];

        yield 'array in an integer column' => [
            schema(int_schema('id')),
            [['id' => [1, 2, 3]]],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "id" (row 0): could not convert array (list<integer>) to integer',
        ];

        yield 'invalid uuid string' => [
            schema(uuid_schema('u')),
            [['u' => 'not-a-uuid']],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "u" (row 0): could not convert \'not-a-uuid\' (string) to uuid',
        ];

        yield 'uppercase uuid string' => [
            schema(uuid_schema('u')),
            [['u' => '01234567-89AB-4DEF-8123-456789ABCDEF']],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "u" (row 0): could not convert \'01234567-89AB-4DEF-8123-456789AB...\' (string) to uuid',
        ];

        yield 'json from a scalar' => [
            schema(json_schema('j')),
            [['j' => 5]],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "j" (row 0): could not convert 5 (integer) to json',
        ];

        yield 'json from an invalid json string' => [
            schema(json_schema('j')),
            [['j' => '{oops']],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "j" (row 0): could not convert \'{oops\' (string) to json',
        ];

        yield 'json from a plain string' => [
            schema(json_schema('j')),
            [['j' => 'plain']],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "j" (row 0): could not convert \'plain\' (string) to json',
        ];

        yield 'datetime from garbage' => [
            schema(datetime_schema('at')),
            [['at' => 'not-a-date']],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "at" (row 0): could not convert \'not-a-date\' (string) to datetime',
        ];

        yield 'datetime from an array' => [
            schema(datetime_schema('at')),
            [['at' => ['nope']]],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "at" (row 0): could not convert array (list<string>) to datetime',
        ];

        yield 'date from garbage' => [
            schema(date_schema('d')),
            [['d' => 'not-a-date']],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "d" (row 0): could not convert \'not-a-date\' (string) to date',
        ];

        yield 'string map with integer keys' => [
            schema(map_schema('m', type_map(type_string(), type_integer()))),
            [['m' => [5 => 1]]],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "m" (row 0): could not convert array (map<integer, integer>) to map<string, integer>',
        ];

        yield 'list with non-sequential keys' => [
            schema(list_schema('l', type_list(type_integer()))),
            [['l' => [1 => 'x']]],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "l" (row 0): could not convert array (map<integer, string>) to list<integer>',
        ];

        yield 'positive integer list from a non-numeric string' => [
            schema(list_schema('l', type_list(type_positive_integer()))),
            [['l' => ['abc']]],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "l" (row 0): could not convert array (list<string>) to list<positive_integer>',
        ];

        yield 'positive integer list from a negative int' => [
            schema(list_schema('l', type_list(type_positive_integer()))),
            [['l' => [-3]]],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "l" (row 0): could not convert array (list<integer>) to list<positive_integer>',
        ];

        yield 'structure missing required element' => [
            schema(structure_schema('data', type_structure(['id' => type_integer(), 'name' => type_string()]))),
            [['data' => ['id' => 1]]],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "data" (row 0): could not convert array (structure{id: integer}) to structure{id: integer, name: string}',
        ];

        yield 'structure present-null required element' => [
            schema(structure_schema('data', type_structure(['id' => type_integer(), 'name' => type_string()]))),
            [['data' => ['id' => 1, 'name' => null]]],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "data" (row 0): could not convert array (structure{id: integer, name: null}) to structure{id: integer, name: string}',
        ];

        yield 'structure present-null optional element' => [
            schema(structure_schema('data', type_structure([
                'id' => type_integer(),
                'name' => structure_element('name', type_string(), optional: true),
            ]))),
            [['data' => ['id' => 1, 'name' => null]]],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "data" (row 0): could not convert array (structure{id: integer, name: null}) to structure{id: integer, name?: string}',
        ];

        // the refusal is placed at the batch position, so a constant row index would diverge here
        yield 'non numeric string in the second row of an integer column' => [
            schema(int_schema('id')),
            [['id' => '1'], ['id' => 'x']],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "id" (row 1): could not convert \'x\' (string) to integer',
        ];

        yield 'a not null column the row does not carry' => [
            schema(int_schema('id'), str_schema('name')),
            [['id' => 1]],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "name" (row 0) declared by the schema is missing from the row',
        ];

        yield 'integer overflow' => [
            schema(int_schema('id')),
            [['id' => '9223372036854775808']],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "id" (row 0): could not convert \'9223372036854775808\' (string) to integer',
        ];

        yield 'date from an empty string' => [
            schema(date_schema('d')),
            [['d' => '']],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "d" (row 0): could not convert \'\' (string) to date',
        ];

        yield 'datetime from a relative word' => [
            schema(datetime_schema('at')),
            [['at' => 'now']],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "at" (row 0): could not convert \'now\' (string) to datetime',
        ];

        yield 'string from a null element inside a list' => [
            schema(list_schema('l', type_list(type_string()))),
            [['l' => ['a', null]]],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "l" (row 0): could not convert array (list<?string>) to list<string>',
        ];

        yield 'list from a scalar' => [
            schema(list_schema('l', type_list(type_integer()))),
            [['l' => 5]],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "l" (row 0): could not convert 5 (integer) to list<integer>',
        ];

        // ordering: an absence must not pre-empt a refusal that PHP reports first, or the two
        // hydrators name different columns - and different ROWS - for the same batch
        yield 'an absent column before a cast refusal in the same row' => [
            schema(int_schema('a'), int_schema('b')),
            [['b' => 'abc']],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "b" (row 0): could not convert \'abc\' (string) to integer',
        ];

        yield 'an absent column in row 0 before a cast refusal in row 1' => [
            schema(int_schema('a'), int_schema('b')),
            [['a' => 1], ['a' => 1, 'b' => 'abc']],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "b" (row 1): could not convert \'abc\' (string) to integer',
        ];

        yield 'an absent column in row 0 before a present null in row 1' => [
            schema(int_schema('a'), int_schema('b')),
            [['a' => 1], ['a' => 1, 'b' => null]],
            SchemaMismatchException::class,
            'Rows do not match their schema: column "b" (row 1): could not convert null to integer, column is not nullable',
        ];
    }

    public function test_a_refused_value_wins_over_an_earlier_absence(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "b" (row 1): could not convert null to integer, column is not nullable');

        (new RowsBuilder(schema(int_schema('a'), int_schema('b')), new PhpBackend()))->appendRows([
            ['a' => 1],
            ['a' => 1, 'b' => null],
        ]);
    }

    public function test_an_absence_is_placed_after_the_rows_already_appended(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "c" (row 2) declared by the schema is missing from the row');

        (new RowsBuilder(
            schema(int_schema('a'), int_schema('b', nullable: true), int_schema('c')),
            new PhpBackend(),
        ))->appendRows([['a' => 0, 'c' => 0]])->appendRows([['a' => 1, 'c' => 1], ['a' => 2]]);
    }

    public function test_the_first_refused_value_in_row_order_across_columns(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "b" (row 1): could not convert \'x\' (string) to integer');

        (new RowsBuilder(schema(int_schema('a'), int_schema('b')), new PhpBackend()))->appendRows([[
            'a' => 0,
            'b' => 0,
        ]])->appendRows([['a' => 1, 'b' => 'x'], ['a' => 'y', 'b' => 2]]);
    }

    public function test_a_view_of_another_definition_keeps_a_value_that_matches_the_target(): void
    {
        static::assertSame(
            [['id' => 5, 'name' => null]],
            (new RowsBuilder(schema(int_schema('id'), str_schema('name', nullable: true)), new PhpBackend()))
                ->appendFrom(array_to_rows([['id' => 5]], schema(int_schema('id', nullable: true))), 0)
                ->finish()
                ->toArray(),
        );
    }

    public function test_a_refused_view_is_placed_at_its_output_row(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "id" (row 1): could not convert \'x\' (string) to integer');

        (new RowsBuilder(schema(int_schema('id')), new PhpBackend()))
            ->appendFrom(array_to_rows([['id' => 1]], schema(int_schema('id'))), 0)
            ->appendFrom(array_to_rows([['id' => 'x']], schema(str_schema('id'))), 0);
    }

    #[DataProvider('refused_views')]
    public function test_refuses_a_view_that_does_not_fit_the_target(
        Rows $source,
        Schema $target,
        string $message,
    ): void {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage($message);

        (new RowsBuilder($target, new PhpBackend()))->appendFrom($source, 0);
    }

    public function test_a_missing_nullable_column_is_padded_with_null(): void
    {
        static::assertEquals(
            array_to_rows([['id' => 1, 'name' => null]], schema(int_schema('id'), str_schema('name', nullable: true))),
            (new RowsBuilder(
                schema(int_schema('id'), str_schema('name', nullable: true)),
                new PhpBackend(),
            ))->appendRows([['id' => 1]])->finish(),
        );
    }

    public function test_a_missing_not_null_column_is_refused_with_its_row(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "name" (row 1) declared by the schema is missing from the row',
        );

        (new RowsBuilder(schema(int_schema('id'), str_schema('name')), new PhpBackend()))->appendRows([
            ['id' => 1, 'name' => 'a'],
            ['id' => 2],
        ]);
    }

    public function test_a_refused_value_in_the_first_row_reports_row_zero(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "i" (row 0): could not convert \'abc\' (string) to integer',
        );

        (new RowsBuilder(schema(int_schema('i')), new PhpBackend()))->appendRows([['i' => 'abc']]);
    }

    public function test_a_refused_value_is_reported_with_its_column_and_row(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "i" (row 1): could not convert \'n/a\' (string) to integer',
        );

        (new RowsBuilder(schema(int_schema('i')), new PhpBackend()))->appendRows([
            ['i' => 1],
            ['i' => 'n/a'],
            ['i' => 3],
        ]);
    }

    public function test_a_refusal_in_a_later_append_reports_its_row_in_the_whole_batch(): void
    {
        $builder = new RowsBuilder(schema(int_schema('i')), new PhpBackend());
        $builder->appendRows([['i' => 1], ['i' => 2]]);

        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "i" (row 3): could not convert \'x\' (string) to integer');

        $builder->appendRows([['i' => 3], ['i' => 'x']]);
    }

    public function test_a_null_on_a_not_null_column_is_reported_as_a_value_that_does_not_match(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage(
            'Rows do not match their schema: column "i" (row 0): could not convert null to integer, column is not nullable',
        );

        (new RowsBuilder(schema(int_schema('i')), new PhpBackend()))->append(['i' => null]);
    }

    public function test_a_value_the_schema_does_not_declare_is_dropped(): void
    {
        static::assertSame(
            ['id'],
            array_keys(
                (new RowsBuilder(schema(int_schema('id')), new PhpBackend()))
                    ->append(['id' => 1, 'undeclared' => 'x'])
                    ->finish()
                    ->values(0),
            ),
        );
    }

    public function test_rows_follow_the_schema_column_order(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => 'a']],
            (new RowsBuilder(schema(int_schema('id'), str_schema('name')), new PhpBackend()))
                ->append(['name' => 'a', 'id' => 1])
                ->finish()
                ->toArray(),
        );
    }

    public function test_every_value_is_cast_against_its_own_column(): void
    {
        static::assertEquals(
            array_to_rows([['id' => 1, 'name' => '2']], schema(int_schema('id'), str_schema('name'))),
            (new RowsBuilder(schema(int_schema('id'), str_schema('name')), new PhpBackend()))->append([
                'id' => '1',
                'name' => 2,
            ])->finish(),
        );
    }

    public function test_a_schema_without_definitions_counts_rows(): void
    {
        $builder = new RowsBuilder(schema(), new PhpBackend());
        $builder->append([]);
        $builder->append([]);
        $builder->appendRows([[]]);

        static::assertSame(3, $builder->count());
        static::assertSame(3, $builder->finish()->count());
    }

    public function test_appending_no_rows_changes_nothing(): void
    {
        static::assertSame(0, (new RowsBuilder(schema(int_schema('id')), new PhpBackend()))->appendRows([])->count());
    }

    public function test_copies_physical_cells_from_another_batch(): void
    {
        $source = array_to_rows(
            [
                ['id' => 1, 'name' => 'a'],
                [
                    'id' => 2,
                    'name' => 'b',
                ],
            ],
            schema(int_schema('id'), str_schema('name')),
        );

        static::assertSame(
            [['id' => 2, 'name' => 'b'], ['id' => 2, 'name' => 'b'], ['id' => 1, 'name' => 'a']],
            (new RowsBuilder(schema(int_schema('id'), str_schema('name')), new PhpBackend()))
                ->appendFrom($source, 1)
                ->appendTake($source, [1, 0])
                ->finish()
                ->toArray(),
        );
    }

    public function test_feeds_one_column(): void
    {
        $builder = new RowsBuilder(schema(int_schema('id')), new PhpBackend());
        $builder->column('id')->appendMany([1, 2]);

        static::assertSame(2, $builder->count());
        static::assertSame([['id' => 1], ['id' => 2]], $builder->finish()->toArray());
    }

    public function test_an_unknown_column_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('RowsBuilder has no column "nope"');

        (new RowsBuilder(schema(int_schema('id')), new PhpBackend()))->column('nope');
    }

    public function test_ragged_columns_are_refused_at_finish(): void
    {
        $builder = new RowsBuilder(schema(int_schema('a'), int_schema('b')), new PhpBackend());
        $builder->column('a')->appendMany([1, 2]);
        $builder->column('b')->append(1);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Ragged columns: 2 rows in [a], 1 rows in [b]');

        $builder->finish();
    }

    /**
     * @param list<array<array-key, mixed>> $rows
     * @param class-string<\Throwable> $exception
     */
    #[DataProvider('refused_rows')]
    public function test_append_rows_refuses(Schema $schema, array $rows, string $exception, string $message): void
    {
        $this->expectException($exception);
        $this->expectExceptionMessage($message);

        (new RowsBuilder($schema, new PhpBackend()))
            ->appendRows($rows)
            ->finish();
    }

    public function test_append_rows_refuses_a_definition_outside_the_nineteen(): void
    {
        $this->expectException(ColumnMismatchException::class);
        $this->expectExceptionMessage(
            'Row does not match its schema: column "a": throwing cannot be a batch column, only the 19 Flow definitions have a column kind',
        );

        (new RowsBuilder(
            schema(
                new ForeignTypeDefinition('a', new ThrowingType(new LogicException('stub type refuses everything'))),
            ),
            new PhpBackend(),
        ))->appendRows([['a' => [1, 2]]]);
    }

    public function test_append_rows_refuses_a_structure_missing_a_required_element(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('Rows do not match their schema: column "data" (row 0)');

        (new RowsBuilder(
            schema(structure_schema('data', type_structure(['id' => type_integer(), 'name' => type_string()]))),
            new PhpBackend(),
        ))->appendRows([['data' => ['id' => 1]]]);
    }

    public function test_append_rows_casts_datetime_and_uuid_strings(): void
    {
        $rows = (new RowsBuilder(
            schema(datetime_schema('created_at'), uuid_schema('uuid')),
            new PhpBackend(),
        ))->appendRows([[
            'created_at' => '2024-01-01 12:00:00 UTC',
            'uuid' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479',
        ]])->finish();

        // @mago-ignore analysis:mixed-assignment
        $uuid = $rows->column('uuid')->value(0);

        static::assertInstanceOf(DateTimeImmutable::class, $rows->column('created_at')->value(0));
        static::assertInstanceOf(Uuid::class, $uuid);
        static::assertSame('f47ac10b-58cc-4372-a567-0e02b2c3d479', $uuid->toString());
    }

    public function test_append_rows_casts_raw_scalar_strings_to_schema_types(): void
    {
        static::assertSame(
            [['id' => 1, 'price' => 9.99, 'active' => true, 'name' => 'Alice']],
            (new RowsBuilder(
                schema(int_schema('id'), float_schema('price'), bool_schema('active'), str_schema('name')),
                new PhpBackend(),
            ))
                ->appendRows([['id' => '1', 'price' => '9.99', 'active' => 'true', 'name' => 'Alice']])
                ->finish()
                ->toArray(),
        );
    }

    public function test_append_rows_keeps_native_typed_values(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => 'Alice'], ['id' => 2, 'name' => 'Bob']],
            (new RowsBuilder(schema(int_schema('id'), str_schema('name')), new PhpBackend()))
                ->appendRows([['id' => 1, 'name' => 'Alice'], ['id' => 2, 'name' => 'Bob']])
                ->finish()
                ->toArray(),
        );
    }

    public function test_append_rows_keeps_native_uuid_value_objects(): void
    {
        // @mago-ignore analysis:mixed-assignment
        $value = (new RowsBuilder(schema(uuid_schema('id')), new PhpBackend()))
            ->appendRows([['id' => new Uuid('f47ac10b-58cc-4372-a567-0e02b2c3d479')]])
            ->finish()
            ->column('id')
            ->value(0);

        static::assertInstanceOf(Uuid::class, $value);
        static::assertSame('f47ac10b-58cc-4372-a567-0e02b2c3d479', $value->toString());
    }

    public function test_append_rows_keeps_the_schema_definition_of_a_null_value(): void
    {
        $rows = (new RowsBuilder(
            schema(int_schema('id'), str_schema('name', nullable: true)),
            new PhpBackend(),
        ))->appendRows([['id' => 1, 'name' => null]])->finish();

        static::assertNull($rows->column('name')->value(0));
        static::assertTrue($rows->schema()->get('name')->isNullable());
    }

    public function test_append_rows_shares_the_declared_schema_instance(): void
    {
        $schema = schema(int_schema('id'));

        static::assertSame(
            $schema,
            (new RowsBuilder($schema, new PhpBackend()))
                ->appendRows([['id' => 1], ['id' => 2]])
                ->finish()
                ->schema(),
        );
    }

    public function test_append_rows_does_not_cast_native_values(): void
    {
        $createdAt = new DateTimeImmutable('2024-01-01 12:00:00 UTC');

        $rows = (new RowsBuilder(
            schema(int_schema('id'), datetime_schema('created_at')),
            new PhpBackend(),
        ))->appendRows([['id' => 1, 'created_at' => $createdAt]])->finish();

        static::assertSame(1, $rows->column('id')->value(0));
        static::assertEquals($createdAt, $rows->column('created_at')->value(0));
    }

    public function test_append_rows_does_not_cast_nested_list_map_and_structure(): void
    {
        $rows = (new RowsBuilder(
            schema(
                list_schema('tags', type_list(type_string())),
                map_schema('counts', type_map(type_string(), type_integer())),
                structure_schema('address', type_structure(['city' => type_string(), 'zip' => type_integer()])),
            ),
            new PhpBackend(),
        ))->appendRows([[
            'tags' => ['a', 'b'],
            'counts' => ['x' => 1, 'y' => 2],
            'address' => ['city' => 'NYC', 'zip' => 10001],
        ]])->finish();

        static::assertInstanceOf(ListType::class, $rows->schema()->get('tags')->type());
        static::assertInstanceOf(MapType::class, $rows->schema()->get('counts')->type());
        static::assertInstanceOf(StructureType::class, $rows->schema()->get('address')->type());
        static::assertSame(['a', 'b'], $rows->column('tags')->value(0));
        static::assertSame(['x' => 1, 'y' => 2], $rows->column('counts')->value(0));
        static::assertSame(['city' => 'NYC', 'zip' => 10001], $rows->column('address')->value(0));
    }

    public function test_append_rows_of_already_typed_values_is_idempotent(): void
    {
        $schema = schema(int_schema('id'), str_schema('name'), datetime_schema('created_at'));
        $rows = (new RowsBuilder($schema, new PhpBackend()))->appendRows([[
            'id' => 1,
            'name' => 'Alice',
            'created_at' => new DateTimeImmutable('2024-01-01 00:00:00 UTC'),
        ]])->finish();

        static::assertEquals(
            $rows,
            (new RowsBuilder($schema, new PhpBackend()))->appendRows([$rows->values(0)])->finish(),
        );
    }

    public function test_a_present_null_and_an_absent_key_are_both_null(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => null], ['id' => 2, 'name' => null], ['id' => 3, 'name' => 'c']],
            (new RowsBuilder(schema(int_schema('id'), str_schema('name', nullable: true)), new PhpBackend()))
                ->appendRows([['id' => 1, 'name' => null], ['id' => 2], ['id' => 3, 'name' => 'c']])
                ->finish()
                ->toArray(),
        );
    }

    public function test_a_refused_value_in_a_column_every_row_carries_is_placed_at_its_row(): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "id" (row 3): could not convert \'x\' (string) to integer');

        (new RowsBuilder(schema(int_schema('id')), new PhpBackend()))->appendRows([['id' => 1]])->appendRows([
            ['id' => 2],
            ['id' => 3],
            ['id' => 'x'],
        ]);
    }

    public function test_append_records_takes_a_single_record(): void
    {
        static::assertSame(
            [['id' => 1]],
            (new RowsBuilder(schema(int_schema('id')), new PhpBackend()))
                ->appendRecords(['id' => 1])
                ->finish()
                ->toArray(),
        );
    }

    public function test_append_records_takes_a_list_of_records(): void
    {
        static::assertSame(
            [['id' => 1], ['id' => 2]],
            (new RowsBuilder(schema(int_schema('id')), new PhpBackend()))
                ->appendRecords([['id' => 1], ['id' => 2]])
                ->finish()
                ->toArray(),
        );
    }

    public function test_append_records_matches_a_numeric_string_key_to_its_declared_name(): void
    {
        static::assertSame(
            [1],
            (new RowsBuilder(schema(int_schema('2024')), new PhpBackend()))
                ->appendRecords([['2024' => 1]])
                ->finish()
                ->column('2024')
                ->values(),
        );
    }

    public function test_append_records_refuses_an_undeclared_key_at_its_row(): void
    {
        try {
            (new RowsBuilder(schema(int_schema('id')), new PhpBackend()))->appendRecords([
                ['id' => 1],
                ['id' => 2, 'nmae' => 'two'],
            ]);
            static::fail('an undeclared key must be refused');
        } catch (SchemaMismatchException $e) {
            static::assertSame(1, $e->rowIndex);
            static::assertStringContainsString('nmae', $e->getMessage());
        }
    }

    public function test_append_records_refuses_an_undeclared_key_at_its_row_after_a_prior_append(): void
    {
        try {
            (new RowsBuilder(schema(int_schema('id')), new PhpBackend()))->appendRecords([
                ['id' => 1],
                ['id' => 2],
            ])->appendRecords([['id' => 3, 'nmae' => 'three']]);
            static::fail('an undeclared key must be refused');
        } catch (SchemaMismatchException $e) {
            static::assertSame(2, $e->rowIndex);
        }
    }

    public function test_append_projected_drops_an_undeclared_key(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => 'one'], ['id' => 2, 'name' => null]],
            (new RowsBuilder(schema(int_schema('id'), str_schema('name', nullable: true)), new PhpBackend()))
                ->appendProjected([['id' => 1, 'name' => 'one'], ['id' => 2, 'nmae' => 'two']])
                ->finish()
                ->toArray(),
        );
    }

    public function test_append_projected_keeps_an_int_key_whose_positional_name_is_declared(): void
    {
        static::assertSame(
            [['e00' => 'a']],
            (new RowsBuilder(schema(str_schema('e00')), new PhpBackend()))
                ->appendProjected([['a', 'b']])
                ->finish()
                ->toArray(),
        );
    }

    public function test_append_projected_keeps_a_numeric_column_name_declared_as_is(): void
    {
        static::assertSame(
            [1],
            (new RowsBuilder(schema(int_schema('2024')), new PhpBackend()))
                ->appendProjected([['2024' => 1, 'other' => 2]])
                ->finish()
                ->column('2024')
                ->values(),
        );
    }

    public function test_append_projected_of_nothing_appends_nothing(): void
    {
        static::assertSame(
            0,
            (new RowsBuilder(schema(int_schema('id')), new PhpBackend()))
                ->appendProjected([])
                ->finish()
                ->count(),
        );
    }
}
