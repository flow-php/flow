<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Formatter;

use Flow\ETL\Row\Formatter\ASCIISchemaFormatter;
use Flow\ETL\Tests\CommandOutputNormalizer;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_element_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ASCIISchemaFormatterTest extends FlowTestCase
{
    use CommandOutputNormalizer;

    public function test_format_nested_schema(): void
    {
        $schema = schema(
            integer_schema('integer', true),
            integer_schema('float'),
            structure_schema('user', type_structure([
                'name' => type_optional(type_string()),
                'age' => type_integer(),
                'address' => type_structure([
                    'street' => type_optional(type_string()),
                    'city' => type_optional(type_string()),
                    'country' => type_optional(type_string()),
                ]),
            ])),
            string_schema('name', nullable: true),
            list_schema('tags', type_list(type_string())),
            bool_schema('active'),
            xml_schema('xml'),
            xml_element_schema('xml_element'),
            json_schema('json'),
            uuid_schema('uuid'),
            datetime_schema('datetime'),
        );

        self::assertCommandOutputIdentical(<<<'SCHEMA'
            schema
            |-- integer: ?integer
            |-- float: integer
            |-- user: structure
            |    |-- name: ?string
            |    |-- age: integer
            |    |-- address: structure
            |        |-- street: ?string
            |        |-- city: ?string
            |        |-- country: ?string
            |-- name: ?string
            |-- tags: list<string>
            |-- active: boolean
            |-- xml: xml
            |-- xml_element: xml_element
            |-- json: json
            |-- uuid: uuid
            |-- datetime: datetime

            SCHEMA, (new ASCIISchemaFormatter())->format($schema));
    }

    public function test_format_schema_with_optional_structure_elements(): void
    {
        $schema = schema(structure_schema('user', type_structure([
            'id' => type_integer(),
            'address' => type_structure([
                'city' => type_string(),
                'zip' => structure_element('zip', type_string(), optional: true),
            ]),
            'nickname' => structure_element('nickname', type_string(), optional: true),
        ])));

        self::assertCommandOutputIdentical(<<<'SCHEMA'
            schema
            |-- user: structure
            |    |-- id: integer
            |    |-- address: structure
            |        |-- city: string
            |        |-- zip?: string
            |    |-- nickname?: string

            SCHEMA, (new ASCIISchemaFormatter())->format($schema));
    }

    public function test_format_nested_schema_as_table(): void
    {
        $schema = schema(
            integer_schema('integer', true),
            integer_schema('float'),
            structure_schema('user', type_structure([
                'name' => type_optional(type_string()),
                'age' => type_integer(),
                'address' => type_structure([
                    'street' => type_optional(type_string()),
                    'city' => type_optional(type_string()),
                    'country' => type_optional(type_string()),
                ]),
            ])),
            string_schema('name', nullable: true),
            list_schema('tags', type_list(type_string())),
            bool_schema('active'),
            xml_schema('xml'),
            xml_element_schema('xml_element'),
            json_schema('json'),
            uuid_schema('uuid'),
            datetime_schema('datetime'),
        );

        self::assertCommandOutputIdentical(<<<'SCHEMA'
            +-------------+--------------+----------+----------+
            |        name |         type | nullable | metadata |
            +-------------+--------------+----------+----------+
            |     integer |      integer |     true |       [] |
            |       float |      integer |    false |       [] |
            |        user | structure_v2 |    false |       [] |
            |        name |       string |     true |       [] |
            |        tags |         list |    false |       [] |
            |      active |      boolean |    false |       [] |
            |         xml |          xml |    false |       [] |
            | xml_element |  xml_element |    false |       [] |
            |        json |         json |    false |       [] |
            |        uuid |         uuid |    false |       [] |
            |    datetime |     datetime |    false |       [] |
            +-------------+--------------+----------+----------+
            11 rows

            SCHEMA, (new ASCIISchemaFormatter(true, true))->format($schema));
    }

    public function test_format_nested_schema_as_table_without_metadata(): void
    {
        $schema = schema(
            integer_schema('integer', true),
            integer_schema('float'),
            structure_schema('user', type_structure([
                'name' => type_optional(type_string()),
                'age' => type_integer(),
                'address' => type_structure([
                    'street' => type_optional(type_string()),
                    'city' => type_optional(type_string()),
                    'country' => type_optional(type_string()),
                ]),
            ])),
            string_schema('name', nullable: true),
            list_schema('tags', type_list(type_string())),
            bool_schema('active'),
            xml_schema('xml'),
            xml_element_schema('xml_element'),
            json_schema('json'),
            uuid_schema('uuid'),
            datetime_schema('datetime'),
        );

        self::assertCommandOutputIdentical(<<<'SCHEMA'
            +-------------+--------------+----------+
            |        name |         type | nullable |
            +-------------+--------------+----------+
            |     integer |      integer |     true |
            |       float |      integer |    false |
            |        user | structure_v2 |    false |
            |        name |       string |     true |
            |        tags |         list |    false |
            |      active |      boolean |    false |
            |         xml |          xml |    false |
            | xml_element |  xml_element |    false |
            |        json |         json |    false |
            |        uuid |         uuid |    false |
            |    datetime |     datetime |    false |
            +-------------+--------------+----------+
            11 rows

            SCHEMA, (new ASCIISchemaFormatter(true, false))->format($schema));
    }

    public function test_format_schema(): void
    {
        $schema = schema(
            string_schema('name', nullable: true),
            list_schema('tags', type_list(type_string())),
            bool_schema('active'),
            xml_schema('xml'),
            map_schema('map', type_map(type_string(), type_string())),
            list_schema('list', type_list(type_map(type_string(), type_integer()))),
        );

        self::assertCommandOutputIdentical(<<<'SCHEMA'
            schema
            |-- name: ?string
            |-- tags: list<string>
            |-- active: boolean
            |-- xml: xml
            |-- map: map<string, string>
            |-- list: list<map<string, integer>>

            SCHEMA, (new ASCIISchemaFormatter())->format($schema));
    }
}
