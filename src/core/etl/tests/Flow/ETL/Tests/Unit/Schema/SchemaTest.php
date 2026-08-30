<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Exception\SchemaDefinitionNotUniqueException;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\SortOrder;
use Flow\ETL\Row\UnresolvedReference;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use JsonException;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_keys;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_from_json;
use function Flow\ETL\DSL\schema_metadata;
use function Flow\ETL\DSL\schema_sort_by_metadata;
use function Flow\ETL\DSL\schema_sort_by_name;
use function Flow\ETL\DSL\schema_sort_by_type;
use function Flow\ETL\DSL\schema_sort_by_type_and_name;
use function Flow\ETL\DSL\schema_to_json;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function json_encode;

final class SchemaTest extends FlowTestCase
{
    public static function provide_add_after_reference_inputs(): Generator
    {
        yield 'string reference' => ['id'];
        yield 'Reference reference' => [ref('id')];
    }

    public static function provide_add_before_reference_inputs(): Generator
    {
        yield 'string reference' => ['name'];
        yield 'Reference reference' => [ref('name')];
    }

    public static function provide_is_same_cases(): Generator
    {
        yield 'identical simple schemas' => [
            schema(int_schema('id'), str_schema('name')),
            schema(int_schema('id'), str_schema('name')),
            true,
        ];

        yield 'different column count' => [
            schema(int_schema('id'), str_schema('name')),
            schema(int_schema('id')),
            false,
        ];

        yield 'different column names' => [
            schema(int_schema('id'), str_schema('name')),
            schema(int_schema('id'), str_schema('surname')),
            false,
        ];

        yield 'different column types' => [
            schema(int_schema('id'), str_schema('name')),
            schema(int_schema('id'), int_schema('name')),
            false,
        ];

        yield 'different nullable flags' => [
            schema(int_schema('id'), str_schema('name', nullable: false)),
            schema(int_schema('id'), str_schema('name', nullable: true)),
            false,
        ];

        yield 'different metadata' => [
            schema(int_schema('id', metadata: Metadata::fromArray(['foo' => 'bar'])), str_schema('name')),
            schema(int_schema('id', metadata: Metadata::fromArray(['foo' => 'baz'])), str_schema('name')),
            false,
        ];

        yield 'empty schemas' => [
            schema(),
            schema(),
            true,
        ];

        yield 'identical nested structure schemas' => [
            schema(structure_schema('address', type_structure(['street' => type_string(), 'city' => type_string()]))),
            schema(structure_schema('address', type_structure(['street' => type_string(), 'city' => type_string()]))),
            true,
        ];

        yield 'different nested structure field types' => [
            schema(structure_schema('address', type_structure(['street' => type_string(), 'city' => type_string()]))),
            schema(structure_schema('address', type_structure(['street' => type_string(), 'city' => type_integer()]))),
            false,
        ];

        yield 'different nested structure field names' => [
            schema(structure_schema('address', type_structure(['street' => type_string(), 'city' => type_string()]))),
            schema(structure_schema('address', type_structure(['street' => type_string(), 'town' => type_string()]))),
            false,
        ];

        yield 'identical list schemas' => [
            schema(list_schema('tags', type_list(type_string()))),
            schema(list_schema('tags', type_list(type_string()))),
            true,
        ];

        yield 'different list element types' => [
            schema(list_schema('tags', type_list(type_string()))),
            schema(list_schema('tags', type_list(type_integer()))),
            false,
        ];

        yield 'identical map schemas' => [
            schema(map_schema('metadata', type_map(type_string(), type_integer()))),
            schema(map_schema('metadata', type_map(type_string(), type_integer()))),
            true,
        ];

        yield 'different map key types' => [
            schema(map_schema('metadata', type_map(type_string(), type_integer()))),
            schema(map_schema('metadata', type_map(type_integer(), type_integer()))),
            false,
        ];

        yield 'different map value types' => [
            schema(map_schema('metadata', type_map(type_string(), type_integer()))),
            schema(map_schema('metadata', type_map(type_string(), type_string()))),
            false,
        ];

        yield 'identical map of list of structure' => [
            schema(map_schema('complex', type_map(
                type_string(),
                type_list(type_structure(['id' => type_integer(), 'name' => type_string()])),
            ))),
            schema(map_schema('complex', type_map(
                type_string(),
                type_list(type_structure(['id' => type_integer(), 'name' => type_string()])),
            ))),
            true,
        ];

        yield 'different nested element in map of list of structure' => [
            schema(map_schema('complex', type_map(
                type_string(),
                type_list(type_structure(['id' => type_integer(), 'name' => type_string()])),
            ))),
            schema(map_schema('complex', type_map(
                type_string(),
                type_list(type_structure(['id' => type_integer(), 'name' => type_integer()])),
            ))),
            false,
        ];

        yield 'deeply nested structure' => [
            schema(structure_schema('root', type_structure([
                'level1' => type_structure([
                    'level2' => type_structure([
                        'value' => type_string(),
                    ]),
                ]),
            ]))),
            schema(structure_schema('root', type_structure([
                'level1' => type_structure([
                    'level2' => type_structure([
                        'value' => type_string(),
                    ]),
                ]),
            ]))),
            true,
        ];

        yield 'different deeply nested structure' => [
            schema(structure_schema('root', type_structure([
                'level1' => type_structure([
                    'level2' => type_structure([
                        'value' => type_string(),
                    ]),
                ]),
            ]))),
            schema(structure_schema('root', type_structure([
                'level1' => type_structure([
                    'level2' => type_structure([
                        'value' => type_integer(),
                    ]),
                ]),
            ]))),
            false,
        ];
    }

    public static function provide_move_after_reference_inputs(): Generator
    {
        yield 'string name, string reference' => ['active', 'id'];
        yield 'string name, Reference reference' => ['active', ref('id')];
        yield 'Reference name, string reference' => [ref('active'), 'id'];
        yield 'Reference name, Reference reference' => [ref('active'), ref('id')];
    }

    public static function provide_move_before_reference_inputs(): Generator
    {
        yield 'string name, string reference' => ['active', 'name'];
        yield 'string name, Reference reference' => ['active', ref('name')];
        yield 'Reference name, string reference' => [ref('active'), 'name'];
        yield 'Reference name, Reference reference' => [ref('active'), ref('name')];
    }

    public static function provide_move_to_reference_inputs(): Generator
    {
        yield 'string name' => ['active'];
        yield 'Reference name' => [ref('active')];
    }

    public static function provide_mutators(): Generator
    {
        yield 'add' => [static fn(Schema $schema) => $schema->add(bool_schema('active'))];
        yield 'addAfter' => [static fn(Schema $schema) => $schema->addAfter('id', bool_schema('active'))];
        yield 'addBefore' => [static fn(Schema $schema) => $schema->addBefore('id', bool_schema('active'))];
        yield 'addMetadata' => [static fn(Schema $schema) => $schema->addMetadata('id', 'primary_key', true)];
        yield 'gracefulRemove' => [static fn(Schema $schema) => $schema->gracefulRemove('name')];
        yield 'insertAt' => [static fn(Schema $schema) => $schema->insertAt(1, bool_schema('active'))];
        yield 'keep' => [static fn(Schema $schema) => $schema->keep('id')];
        yield 'makeNullable' => [static fn(Schema $schema) => $schema->makeNullable()];
        yield 'merge' => [static fn(Schema $schema) => $schema->merge(schema(bool_schema('active')))];
        yield 'moveAfter' => [static fn(Schema $schema) => $schema->moveAfter('id', 'name')];
        yield 'moveBefore' => [static fn(Schema $schema) => $schema->moveBefore('name', 'id')];
        yield 'moveTo' => [static fn(Schema $schema) => $schema->moveTo('name', 0)];
        yield 'prepend' => [static fn(Schema $schema) => $schema->prepend(bool_schema('active'))];
        yield 'remove' => [static fn(Schema $schema) => $schema->remove('name')];
        yield 'rename' => [static fn(Schema $schema) => $schema->rename('name', 'title')];
        yield 'reorder' => [static fn(Schema $schema) => $schema->reorder('name', 'id')];
        yield 'replace' => [static fn(Schema $schema) => $schema->replace('name', str_schema('title'))];
        yield 'setMetadata' => [
            static fn(Schema $schema) => $schema->setMetadata('id', schema_metadata(['primary_key' => true])),
        ];
        yield 'sort' => [static fn(Schema $schema) => $schema->sort()];
    }

    public static function provide_reorder_reference_inputs(): Generator
    {
        yield 'string names' => [['id', 'name', 'email']];
        yield 'Reference names' => [[ref('id'), ref('name'), ref('email')]];
        yield 'mixed names' => [[ref('id'), 'name', ref('email')]];
    }

    #[DataProvider('provide_add_after_reference_inputs')]
    public function test_add_after(string|Reference $reference): void
    {
        $schema = schema(int_schema('id'), str_schema('name'))->addAfter($reference, bool_schema('active'));

        static::assertSame(['id', 'active', 'name'], array_keys($schema->definitions()));
    }

    public function test_add_after_duplicate_definition(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);

        schema(int_schema('id'), str_schema('name'))->addAfter('name', int_schema('id'));
    }

    public function test_add_after_non_existing_reference(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        schema(int_schema('id'), str_schema('name'))->addAfter('not-existing', bool_schema('active'));
    }

    #[DataProvider('provide_add_before_reference_inputs')]
    public function test_add_before(string|Reference $reference): void
    {
        $schema = schema(int_schema('id'), str_schema('name'))->addBefore(
            $reference,
            bool_schema('active'),
            str_schema('email'),
        );

        static::assertSame(['id', 'active', 'email', 'name'], array_keys($schema->definitions()));
    }

    public function test_add_before_duplicate_definition(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);

        schema(int_schema('id'), str_schema('name'))->addBefore('name', int_schema('id'));
    }

    public function test_add_before_non_existing_reference(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        schema(int_schema('id'), str_schema('name'))->addBefore('not-existing', bool_schema('active'));
    }

    public function test_add_metadata(): void
    {
        $schema = schema(int_schema('id'), str_schema('name'));

        static::assertEquals(
            int_schema('id', metadata: Metadata::fromArray(['test' => 'test'])),
            $schema->addMetadata('id', 'test', 'test')->get('id'),
        );
    }

    public function test_adding_duplicated_definitions(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);
        $this->expectExceptionMessage(
            'Entry definitions must be unique, duplicated entries: [str], all: [id, str, str]',
        );
        schema(int_schema('id'), str_schema('str', true))->add(int_schema('str'));
    }

    public function test_adding_new_definitions(): void
    {
        $schema = schema(int_schema('id'), str_schema('str', true))->add(int_schema('number'), bool_schema('bool'));

        static::assertEquals(
            schema(int_schema('id'), str_schema('str', true), int_schema('number'), bool_schema('bool')),
            $schema,
        );
    }

    public function test_allowing_only_unique_definitions(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);

        schema(integer_schema('id'), string_schema('id'));
    }

    public function test_allowing_only_unique_definitions_case_insensitive(): void
    {
        $schema = schema(integer_schema('id'), integer_schema('Id'));

        static::assertEquals(
            refs(UnresolvedReference::init('id'), UnresolvedReference::init('Id')),
            $schema->references(),
        );
    }

    public function test_creating_schema_from_corrupted_json(): void
    {
        $this->expectException(JsonException::class);
        $this->expectExceptionMessage('Syntax error');

        schema_from_json('{"ref": "id", "type": {"type": "integer", "nullable": false}, "metadata": []');
    }

    public function test_creating_schema_from_invalid_json_format(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Schema definition must be an array');

        schema_from_json('{"ref": "id", "type": {"type": "integer", "nullable": false}, "metadata": []}');
    }

    public function test_creating_schema_from_invalid_json_format_at_definition_level(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Definition array must have an array "type" key');

        schema_from_json('[{"ref": "id", "type": "test", "metadata": []}]');
    }

    public function test_get(): void
    {
        $schema = schema(int_schema('id'), str_schema('name'));

        static::assertEquals(int_schema('id'), $schema->get('id'));
    }

    public function test_graceful_remove_non_existing_definition(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('name')),
            schema(int_schema('id'), str_schema('name'))->gracefulRemove('not-existing'),
        );
    }

    public function test_insert_at(): void
    {
        $schema = schema(int_schema('id'), str_schema('name'))->insertAt(1, bool_schema('active'));

        static::assertSame(['id', 'active', 'name'], array_keys($schema->definitions()));
    }

    public function test_insert_at_appends_when_index_equals_count(): void
    {
        $schema = schema(int_schema('id'), str_schema('name'))->insertAt(2, bool_schema('active'));

        static::assertSame(['id', 'name', 'active'], array_keys($schema->definitions()));
    }

    public function test_insert_at_duplicate_definition(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);

        schema(int_schema('id'), str_schema('name'))->insertAt(1, int_schema('id'));
    }

    public function test_insert_at_negative_index(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot insert definitions at index -1, schema has 2 definitions');

        schema(int_schema('id'), str_schema('name'))->insertAt(-1, bool_schema('active'));
    }

    public function test_insert_at_out_of_range(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot insert definitions at index 3, schema has 2 definitions');

        schema(int_schema('id'), str_schema('name'))->insertAt(3, bool_schema('active'));
    }

    public function test_insert_at_prepends_at_zero(): void
    {
        $schema = schema(int_schema('id'), str_schema('name'))->insertAt(0, bool_schema('active'));

        static::assertSame(['active', 'id', 'name'], array_keys($schema->definitions()));
    }

    #[DataProvider('provide_is_same_cases')]
    public function test_is_same(Schema $schema1, Schema $schema2, bool $expected): void
    {
        static::assertSame($expected, $schema1->isSame($schema2));
    }

    public function test_keep_non_existing_entries(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        schema(int_schema('id'), str_schema('name'), str_schema('surname'), str_schema('email'))->keep('not-existing');
    }

    public function test_keep_selected_entries(): void
    {
        $schema = schema(int_schema('id'), str_schema('name'), str_schema('surname'), str_schema('email'));

        static::assertEquals(schema(str_schema('name'), str_schema('surname')), $schema->keep('name', 'surname'));
    }

    public function test_making_whole_schema_nullable(): void
    {
        $schema = schema(integer_schema('id', false), string_schema('name', true));

        static::assertEquals(schema(integer_schema('id', true), string_schema('name', true)), $schema->makeNullable());
    }

    public function test_merge_returns_self_when_schemas_are_identical(): void
    {
        $schema1 = schema(int_schema('id'), str_schema('name'));

        $schema2 = schema(int_schema('id'), str_schema('name'));

        $merged = $schema1->merge($schema2);

        static::assertSame($schema1, $merged);
    }

    #[DataProvider('provide_move_after_reference_inputs')]
    public function test_move_after(string|Reference $name, string|Reference $reference): void
    {
        $schema = schema(int_schema('id'), str_schema('name'), bool_schema('active'))->moveAfter($name, $reference);

        static::assertSame(['id', 'active', 'name'], array_keys($schema->definitions()));
    }

    public function test_move_after_forward(): void
    {
        $schema = schema(int_schema('id'), str_schema('name'), bool_schema('active'))->moveAfter('id', 'name');

        static::assertSame(['name', 'id', 'active'], array_keys($schema->definitions()));
    }

    public function test_move_after_non_existing_entry(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        schema(int_schema('id'), str_schema('name'))->moveAfter('not-existing', 'name');
    }

    public function test_move_after_non_existing_reference(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        schema(int_schema('id'), str_schema('name'))->moveAfter('name', 'not-existing');
    }

    #[DataProvider('provide_move_before_reference_inputs')]
    public function test_move_before(string|Reference $name, string|Reference $reference): void
    {
        $schema = schema(int_schema('id'), str_schema('name'), bool_schema('active'))->moveBefore($name, $reference);

        static::assertSame(['id', 'active', 'name'], array_keys($schema->definitions()));
    }

    public function test_move_before_forward(): void
    {
        $schema = schema(int_schema('id'), str_schema('name'), bool_schema('active'))->moveBefore('id', 'active');

        static::assertSame(['name', 'id', 'active'], array_keys($schema->definitions()));
    }

    public function test_move_before_non_existing_entry(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        schema(int_schema('id'), str_schema('name'))->moveBefore('not-existing', 'name');
    }

    public function test_move_before_non_existing_reference(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        schema(int_schema('id'), str_schema('name'))->moveBefore('name', 'not-existing');
    }

    public function test_move_preserves_definition_metadata(): void
    {
        $schema = schema(
            str_schema('name'),
            int_schema('id', metadata: Metadata::fromArray(['primary_key' => true])),
        )->moveTo('id', 0);

        static::assertSame(['id', 'name'], array_keys($schema->definitions()));
        static::assertEquals(
            int_schema('id', metadata: Metadata::fromArray(['primary_key' => true])),
            $schema->get('id'),
        );
    }

    public function test_move_relative_to_itself(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot move entry "id" relative to itself');

        schema(int_schema('id'), str_schema('name'))->moveBefore('id', 'id');
    }

    #[DataProvider('provide_move_to_reference_inputs')]
    public function test_move_to(string|Reference $name): void
    {
        $schema = schema(int_schema('id'), str_schema('name'), bool_schema('active'))->moveTo($name, 0);

        static::assertSame(['active', 'id', 'name'], array_keys($schema->definitions()));
    }

    public function test_move_to_forward(): void
    {
        $schema = schema(int_schema('id'), str_schema('name'), bool_schema('active'))->moveTo('id', 2);

        static::assertSame(['name', 'active', 'id'], array_keys($schema->definitions()));
    }

    public function test_move_to_non_existing(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        schema(int_schema('id'), str_schema('name'))->moveTo('not-existing', 0);
    }

    public function test_move_to_out_of_range(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot move entry "id" to index 2, schema has 2 definitions');

        schema(int_schema('id'), str_schema('name'))->moveTo('id', 2);
    }

    /**
     * @param callable(Schema) : Schema $mutator
     */
    #[DataProvider('provide_mutators')]
    public function test_mutators_return_new_instance(callable $mutator): void
    {
        $schema = schema(int_schema('id'), str_schema('name'));
        $before = $schema->normalize();

        static::assertNotSame($schema, $mutator($schema));
        static::assertSame($before, $schema->normalize());
    }

    public function test_normalizing_and_recreating_schema(): void
    {
        $schema = schema(
            int_schema('id'),
            str_schema('str', true),
            uuid_schema('uuid'),
            json_schema('json', true),
            map_schema('map', type_map(type_string(), type_integer())),
            list_schema('list', type_list(type_integer())),
            structure_schema('struct', type_structure([
                'street' => type_string(),
                'city' => type_string(),
            ])),
        );

        static::assertEquals($schema, Schema::fromArray($schema->normalize()));
    }

    public function test_prepend(): void
    {
        $schema = schema(
            str_schema('name'),
            str_schema('email'),
        )->prepend(int_schema('id', metadata: Metadata::fromArray(['primary_key' => true])));

        static::assertSame(['id', 'name', 'email'], array_keys($schema->definitions()));
        static::assertEquals(
            int_schema('id', metadata: Metadata::fromArray(['primary_key' => true])),
            $schema->get('id'),
        );
    }

    public function test_prepend_duplicate_definition(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);

        schema(int_schema('id'), str_schema('name'))->prepend(int_schema('id'));
    }

    public function test_prepend_multiple(): void
    {
        $schema = schema(int_schema('id'))->prepend(str_schema('name'), bool_schema('active'));

        static::assertSame(['name', 'active', 'id'], array_keys($schema->definitions()));
    }

    public function test_remove_non_existing_definition(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        schema(int_schema('id'), str_schema('name'))->remove('not-existing');
    }

    public function test_removing_elements_from_schema(): void
    {
        static::assertEquals(schema(int_schema('id')), schema(int_schema('id'), str_schema('name'))->remove('name'));
    }

    public function test_rename(): void
    {
        $schema = schema(int_schema('id'), str_schema('name'));

        static::assertEquals(schema(int_schema('id'), str_schema('new_name')), $schema->rename('name', 'new_name'));
    }

    public function test_rename_non_existing(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        schema(int_schema('id'), str_schema('name'))->rename('not-existing', 'new_name');
    }

    /**
     * @param list<string|Reference> $names
     */
    #[DataProvider('provide_reorder_reference_inputs')]
    public function test_reorder(array $names): void
    {
        $schema = schema(str_schema('name'), str_schema('email'), int_schema('id'))->reorder(...$names);

        static::assertSame(['id', 'name', 'email'], array_keys($schema->definitions()));
    }

    public function test_reorder_duplicate_name(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot reorder entry "id" more than once');

        schema(int_schema('id'), str_schema('name'))->reorder('id', 'id');
    }

    public function test_reorder_keeps_unlisted_columns(): void
    {
        $schema = schema(str_schema('name'), str_schema('email'), int_schema('id'), bool_schema('active'))->reorder(
            'id',
        );

        static::assertSame(['id', 'name', 'email', 'active'], array_keys($schema->definitions()));
    }

    public function test_reorder_non_existing(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        schema(int_schema('id'), str_schema('name'))->reorder('not-existing');
    }

    public function test_replace_non_existing_reference(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        schema(int_schema('id'), str_schema('str', true))->replace('not-existing', int_schema('number'));
    }

    public function test_replace_reference(): void
    {
        $schema = schema(int_schema('id'), str_schema('str', true))->replace('str', int_schema('number'));

        static::assertEquals(schema(int_schema('id'), int_schema('number')), $schema);
    }

    public function test_schema_to_from_json(): void
    {
        $schema = schema(
            int_schema('id'),
            str_schema('str', true),
            uuid_schema('uuid'),
            json_schema('json', true),
            map_schema('map', type_map(type_string(), type_integer())),
            list_schema('list', type_list(type_integer())),
            structure_schema('struct', type_structure([
                'street' => type_string(),
                'city' => type_string(),
            ])),
        );

        static::assertSame(<<<'JSON'
            [
                {
                    "ref": "id",
                    "type": {
                        "type": "integer"
                    },
                    "nullable": false,
                    "metadata": []
                },
                {
                    "ref": "str",
                    "type": {
                        "type": "string"
                    },
                    "nullable": true,
                    "metadata": []
                },
                {
                    "ref": "uuid",
                    "type": {
                        "type": "uuid"
                    },
                    "nullable": false,
                    "metadata": []
                },
                {
                    "ref": "json",
                    "type": {
                        "type": "json"
                    },
                    "nullable": true,
                    "metadata": []
                },
                {
                    "ref": "map",
                    "type": {
                        "type": "map",
                        "key": {
                            "type": "string"
                        },
                        "value": {
                            "type": "integer"
                        }
                    },
                    "nullable": false,
                    "metadata": []
                },
                {
                    "ref": "list",
                    "type": {
                        "type": "list",
                        "element": {
                            "type": "integer"
                        }
                    },
                    "nullable": false,
                    "metadata": []
                },
                {
                    "ref": "struct",
                    "type": {
                        "type": "structure_v2",
                        "fields": [
                            {
                                "name": "street",
                                "type": {
                                    "type": "string"
                                },
                                "optional": false
                            },
                            {
                                "name": "city",
                                "type": {
                                    "type": "string"
                                },
                                "optional": false
                            }
                        ],
                        "allow_extra": false
                    },
                    "nullable": false,
                    "metadata": []
                }
            ]
            JSON, json_encode($schema->normalize(), JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT));

        static::assertEquals($schema, schema_from_json(schema_to_json($schema)));
    }

    public function test_set_metadata(): void
    {
        $schema = schema(int_schema('id', metadata: Metadata::fromArray(['foo' => 'bar'])), str_schema('name'));

        static::assertEquals(
            int_schema('id', metadata: Metadata::fromArray(['test' => 'test'])),
            $schema->setMetadata('id', Metadata::fromArray(['test' => 'test']))->get('id'),
        );
    }

    public function test_sort_alphabetically_by_default(): void
    {
        $schema = schema(str_schema('name'), int_schema('id'), bool_schema('active'));

        static::assertSame(['active', 'id', 'name'], array_keys($schema->sort()->definitions()));
    }

    public function test_sort_by_metadata(): void
    {
        $schema = schema(
            int_schema('c', metadata: schema_metadata(['group' => 2])),
            int_schema('a', metadata: schema_metadata(['group' => 1])),
            int_schema('b'),
        );

        static::assertSame(['a', 'c', 'b'], array_keys($schema->sort(schema_sort_by_metadata('group'))->definitions()));
    }

    public function test_sort_by_name_descending(): void
    {
        $schema = schema(str_schema('name'), int_schema('id'), bool_schema('active'));

        static::assertSame(
            ['name', 'id', 'active'],
            array_keys($schema->sort(schema_sort_by_name(SortOrder::DESC))->definitions()),
        );
    }

    public function test_sort_by_type(): void
    {
        $schema = schema(str_schema('name'), bool_schema('active'), int_schema('id'));

        static::assertSame(['id', 'active', 'name'], array_keys($schema->sort(schema_sort_by_type())->definitions()));
    }

    public function test_sort_by_type_and_name(): void
    {
        $schema = schema(int_schema('z'), str_schema('m'), int_schema('a'));

        static::assertSame(['a', 'z', 'm'], array_keys($schema->sort(schema_sort_by_type_and_name())->definitions()));
    }

    public function test_sort_empty_schema(): void
    {
        static::assertSame([], array_keys(schema()->sort()->definitions()));
    }

    public function test_conform_order_to_reorders_columns_and_structure_fields(): void
    {
        $schema = schema(
            structure_schema('s', type_structure(['b' => type_string(), 'a' => type_integer()])),
            int_schema('id'),
        );
        $authority = schema(
            int_schema('id'),
            structure_schema('s', type_structure(['a' => type_integer(), 'b' => type_string()])),
        );

        $conformed = $schema->conformOrderTo($authority);

        static::assertSame(['id', 's'], array_keys($conformed->definitions()));
        static::assertSame('structure{a: integer, b: string}', $conformed->get('s')->type()->toString());
    }

    public function test_conform_order_to_appends_columns_the_authority_does_not_know(): void
    {
        static::assertSame(
            ['id', 'extra'],
            array_keys(
                schema(int_schema('id'), str_schema('extra'))->conformOrderTo(schema(int_schema('id')))->definitions(),
            ),
        );
    }

    public function test_conform_order_to_adds_and_drops_nothing(): void
    {
        static::assertSame(
            ['id'],
            array_keys(
                schema(int_schema('id'))->conformOrderTo(schema(int_schema('id'), str_schema('name')))->definitions(),
            ),
        );
    }
}
