<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Exception\SchemaDefinitionNotUniqueException;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use JsonException;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_from_json;
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

        static::assertEquals(refs(EntryReference::init('id'), EntryReference::init('Id')), $schema->references());
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
                        "type": "structure",
                        "elements": {
                            "street": {
                                "type": "string"
                            },
                            "city": {
                                "type": "string"
                            }
                        },
                        "optional_elements": [],
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
}
