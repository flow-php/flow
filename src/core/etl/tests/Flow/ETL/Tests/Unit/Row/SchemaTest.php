<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use function Flow\ETL\DSL\{bool_schema,
    int_schema,
    json_schema,
    list_schema,
    map_schema,
    refs,
    schema,
    schema_evolving_matcher,
    schema_from_json,
    schema_strict_matcher,
    schema_to_json,
    str_schema,
    struct_element,
    structure_schema,
    type_int,
    type_list,
    type_map,
    type_string,
    type_structure,
    uuid_schema};
use Flow\ETL\Exception\{InvalidArgumentException, SchemaDefinitionNotFoundException, SchemaDefinitionNotUniqueException};
use Flow\ETL\Row\{EntryReference, Schema};
use Flow\ETL\Tests\FlowTestCase;

final class SchemaTest extends FlowTestCase
{
    public function test_adding_duplicated_definitions() : void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);
        schema(
            int_schema('id'),
            str_schema('str', true),
        )->add(int_schema('str'));
    }

    public function test_adding_new_definitions() : void
    {
        $schema = schema(
            int_schema('id'),
            str_schema('str', true),
        )->add(int_schema('number'), bool_schema('bool'));

        self::assertEquals(
            schema(
                int_schema('id'),
                str_schema('str', true),
                int_schema('number'),
                bool_schema('bool'),
            ),
            $schema
        );
    }

    public function test_allowing_only_unique_definitions() : void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);

        new Schema(
            Schema\Definition::integer('id'),
            Schema\Definition::string('id')
        );
    }

    public function test_allowing_only_unique_definitions_case_insensitive() : void
    {
        $schema = new Schema(
            Schema\Definition::integer('id'),
            Schema\Definition::integer('Id')
        );

        self::assertEquals(refs(EntryReference::init('id'), EntryReference::init('Id')), $schema->references());
    }

    public function test_creating_schema_from_corrupted_json() : void
    {
        $this->expectException(\JsonException::class);
        $this->expectExceptionMessage('Syntax error');

        schema_from_json('{"ref": "id", "type": {"type": "integer", "nullable": false}, "metadata": []');
    }

    public function test_creating_schema_from_invalid_json_format() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Schema definition must be an array');

        schema_from_json('{"ref": "id", "type": {"type": "integer", "nullable": false}, "metadata": []}');
    }

    public function test_creating_schema_from_invalid_json_format_at_definition_level() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Schema definition "type" must be an array, got: "test"');

        schema_from_json('[{"ref": "id", "type": "test", "metadata": []}]');
    }

    public function test_graceful_remove_non_existing_definition() : void
    {

        self::assertEquals(
            schema(
                int_schema('id'),
                str_schema('name'),
            ),
            schema(
                int_schema('id'),
                str_schema('name'),
            )->gracefulRemove('not-existing')
        );
    }

    public function test_keep_non_existing_entries() : void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        schema(
            int_schema('id'),
            str_schema('name'),
            str_schema('surname'),
            str_schema('email'),
        )->keep('not-existing');
    }

    public function test_keep_selected_entries() : void
    {
        $schema = schema(
            int_schema('id'),
            str_schema('name'),
            str_schema('surname'),
            str_schema('email'),
        );

        self::assertEquals(
            schema(
                str_schema('name'),
                str_schema('surname'),
            ),
            $schema->keep('name', 'surname')
        );
    }

    public function test_making_whole_schema_nullable() : void
    {
        $schema = new Schema(
            Schema\Definition::integer('id', $nullable = false),
            Schema\Definition::string('name', $nullable = true)
        );

        self::assertEquals(
            new Schema(
                Schema\Definition::integer('id', $nullable = true),
                Schema\Definition::string('name', $nullable = true)
            ),
            $schema->makeNullable()
        );
    }

    public function test_matching_schema_with_evolving_schema_matcher() : void
    {
        $left = schema(
            int_schema('id'),
            str_schema('name'),
        );

        $right = schema(
            int_schema('id'),
            str_schema('name'),
            str_schema('surname'),
        );

        self::assertTrue($left->matches($right, schema_evolving_matcher()));
    }

    public function test_matching_schema_with_strict_schema_matcher() : void
    {
        $left = schema(
            int_schema('id'),
            str_schema('name'),
        );

        $right = schema(
            int_schema('id'),
            str_schema('name'),
            str_schema('surname'),
        );

        self::assertFalse($left->matches($right, schema_strict_matcher()));
    }

    public function test_normalizing_and_recreating_schema() : void
    {
        $schema = schema(
            int_schema('id'),
            str_schema('str', true),
            uuid_schema('uuid'),
            json_schema('json', true),
            map_schema('map', type_map(type_string(), type_int())),
            list_schema('list', type_list(type_int())),
            structure_schema('struct', type_structure([
                struct_element('street', type_string()),
                struct_element('city', type_string()),
            ]))
        );

        self::assertEquals(
            $schema,
            Schema::fromArray($schema->normalize())
        );
    }

    public function test_remove_non_existing_definition() : void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        schema(
            int_schema('id'),
            str_schema('name'),
        )->remove('not-existing');
    }

    public function test_removing_elements_from_schema() : void
    {
        self::assertEquals(
            schema(
                int_schema('id'),
            ),
            schema(
                int_schema('id'),
                str_schema('name'),
            )->remove('name')
        );
    }

    public function test_rename() : void
    {
        $schema = schema(
            int_schema('id'),
            str_schema('name'),
        );

        self::assertEquals(
            schema(
                int_schema('id'),
                str_schema('new_name'),
            ),
            $schema->rename('name', 'new_name')
        );
    }

    public function test_rename_non_existing() : void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        schema(
            int_schema('id'),
            str_schema('name'),
        )->rename('not-existing', 'new_name');
    }

    public function test_replace_non_existing_reference() : void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        schema(
            int_schema('id'),
            str_schema('str', true),
        )->replace('not-existing', int_schema('number'));
    }

    public function test_replace_reference() : void
    {
        $schema = schema(
            int_schema('id'),
            str_schema('str', true),
        )->replace('str', int_schema('number'));

        self::assertEquals(
            schema(
                int_schema('id'),
                int_schema('number'),
            ),
            $schema
        );
    }

    public function test_schema_to_from_json() : void
    {
        $schema = schema(
            int_schema('id'),
            str_schema('str', true),
            uuid_schema('uuid'),
            json_schema('json', true),
            map_schema('map', type_map(type_string(), type_int())),
            list_schema('list', type_list(type_int())),
            structure_schema('struct', type_structure([
                struct_element('street', type_string()),
                struct_element('city', type_string()),
            ]))
        );

        self::assertSame(
            <<<'JSON'
[
    {
        "ref": "id",
        "type": {
            "type": "integer",
            "nullable": false
        },
        "metadata": []
    },
    {
        "ref": "str",
        "type": {
            "type": "string",
            "nullable": true
        },
        "metadata": []
    },
    {
        "ref": "uuid",
        "type": {
            "type": "uuid",
            "nullable": false
        },
        "metadata": []
    },
    {
        "ref": "json",
        "type": {
            "type": "json",
            "nullable": true
        },
        "metadata": []
    },
    {
        "ref": "map",
        "type": {
            "type": "map",
            "key": {
                "type": {
                    "type": "string",
                    "nullable": false
                }
            },
            "value": {
                "type": {
                    "type": "integer",
                    "nullable": false
                }
            },
            "nullable": false
        },
        "metadata": []
    },
    {
        "ref": "list",
        "type": {
            "type": "list",
            "element": {
                "type": {
                    "type": "integer",
                    "nullable": false
                }
            },
            "nullable": false
        },
        "metadata": []
    },
    {
        "ref": "struct",
        "type": {
            "type": "structure",
            "elements": [
                {
                    "name": "street",
                    "type": {
                        "type": "string",
                        "nullable": false
                    }
                },
                {
                    "name": "city",
                    "type": {
                        "type": "string",
                        "nullable": false
                    }
                }
            ],
            "nullable": false
        },
        "metadata": []
    }
]
JSON,
            \json_encode($schema->normalize(), JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT)
        );

        self::assertEquals(
            $schema,
            schema_from_json(schema_to_json($schema))
        );
    }
}
