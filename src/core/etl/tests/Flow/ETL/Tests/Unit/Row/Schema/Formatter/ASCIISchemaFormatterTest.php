<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema\Formatter;

use function Flow\ETL\DSL\{bool_schema, datetime_schema, integer_schema, json_schema, list_schema, map_schema, schema, string_schema, structure_element, structure_schema, type_integer, type_map, type_structure, uuid_schema, xml_element_schema, xml_schema};
use function Flow\ETL\DSL\{type_int, type_list, type_string};
use Flow\ETL\Row\Schema\Formatter\ASCIISchemaFormatter;
use Flow\ETL\Tests\FlowTestCase;

final class ASCIISchemaFormatterTest extends FlowTestCase
{
    public function test_format_nested_schema() : void
    {
        $schema = schema(integer_schema('integer', true), integer_schema('float'), structure_schema('user', type_structure([
            structure_element('name', type_string(true)),
            structure_element('age', type_int()),
            structure_element('address', type_structure([
                structure_element('street', type_string(true)),
                structure_element('city', type_string(true)),
                structure_element('country', type_string(true)),
            ])),
        ])), string_schema('name', nullable: true), list_schema('tags', type_list(type_string())), bool_schema('active'), xml_schema('xml'), xml_element_schema('xml_element'), json_schema('json'), uuid_schema('uuid'), datetime_schema('datetime'));

        self::assertSame(
            <<<'SCHEMA'
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

SCHEMA,
            (new ASCIISchemaFormatter())->format($schema)
        );
    }

    public function test_format_schema() : void
    {
        $schema = schema(string_schema('name', nullable: true), list_schema('tags', type_list(type_string())), bool_schema('active'), xml_schema('xml'), map_schema('map', type_map(type_string(), type_string())), list_schema('list', type_list(type_map(type_string(), type_integer()))));

        self::assertSame(
            <<<'SCHEMA'
schema
|-- name: ?string
|-- tags: list<string>
|-- active: boolean
|-- xml: xml
|-- map: map<string, string>
|-- list: list<map<string, integer>>

SCHEMA,
            (new ASCIISchemaFormatter())->format($schema)
        );
    }
}
