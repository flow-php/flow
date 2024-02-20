<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_from_json;
use function Flow\ETL\DSL\schema_to_json;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\struct_element;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_list;
use function Flow\ETL\DSL\type_map;
use function Flow\ETL\DSL\type_string;
use function Flow\ETL\DSL\type_structure;
use function Flow\ETL\DSL\uuid_schema;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Exception\SchemaDefinitionNotUniqueException;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Schema;
use PHPUnit\Framework\TestCase;

final class SchemaTest extends TestCase
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

        $this->assertEquals(
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

        $this->assertEquals([EntryReference::init('id'), EntryReference::init('Id')], $schema->entries());
    }

    public function test_creating_schema_from_corrupted_json() : void
    {
        $this->expectException(\JsonException::class);
        $this->expectExceptionMessage('Syntax error');

        schema_from_json('{"ref": "id", "type": {"type": "scalar", "scalar_type": "integer", "nullable": false}, "metadata": []');
    }

    public function test_creating_schema_from_invalid_json_format() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Schema definition must be an array');

        schema_from_json('{"ref": "id", "type": {"type": "scalar", "scalar_type": "integer", "nullable": false}, "metadata": []}');
    }

    public function test_creating_schema_from_invalid_json_format_at_definition_level() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Schema definition "type" must be an array, got: "test"');

        schema_from_json('[{"ref": "id", "type": "test", "metadata": []}]');
    }

    public function test_graceful_remove_non_existing_definition() : void
    {

        $this->assertEquals(
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

        $this->assertEquals(
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

        $this->assertEquals(
            new Schema(
                Schema\Definition::integer('id', $nullable = true),
                Schema\Definition::string('name', $nullable = true)
            ),
            $schema->nullable()
        );
    }

    public function test_merge_int_empty_schema() : void
    {
        $schema = (new Schema())->merge(
            $notEmptySchema = new Schema(
                Schema\Definition::integer('id', $nullable = true),
                Schema\Definition::string('name', $nullable = true)
            )
        );

        $this->assertSame(
            $notEmptySchema,
            $schema
        );
    }

    public function test_merge_schema() : void
    {
        $schema = (new Schema(
            Schema\Definition::integer('id', $nullable = true),
            Schema\Definition::string('name', $nullable = true)
        ))->merge(
            new Schema(
                Schema\Definition::null('test'),
            )
        );

        $this->assertEquals(
            new Schema(
                Schema\Definition::integer('id', $nullable = true),
                Schema\Definition::string('name', $nullable = true),
                Schema\Definition::null('test'),
            ),
            $schema
        );
    }

    public function test_merge_with_empty_schema() : void
    {
        $schema = ($notEmptySchema = new Schema(
            Schema\Definition::integer('id', $nullable = true),
            Schema\Definition::string('name', $nullable = true)
        ))->merge(
            new Schema()
        );

        $this->assertEquals(
            $notEmptySchema,
            $schema
        );
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

        $this->assertEquals(
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
        $this->assertEquals(
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

        $this->assertEquals(
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

        $this->assertEquals(
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

        $this->assertSame(
            <<<'JSON'
[
    {
        "ref": "id",
        "type": {
            "type": "scalar",
            "scalar_type": "integer",
            "nullable": false
        },
        "metadata": []
    },
    {
        "ref": "str",
        "type": {
            "type": "scalar",
            "scalar_type": "string",
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
                    "type": "scalar",
                    "scalar_type": "string",
                    "nullable": false
                }
            },
            "value": {
                "type": {
                    "type": "scalar",
                    "scalar_type": "integer",
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
                    "type": "scalar",
                    "scalar_type": "integer",
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
                        "type": "scalar",
                        "scalar_type": "string",
                        "nullable": false
                    }
                },
                {
                    "name": "city",
                    "type": {
                        "type": "scalar",
                        "scalar_type": "string",
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

        $this->assertEquals(
            $schema,
            schema_from_json(schema_to_json($schema))
        );
    }
}
