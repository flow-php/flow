<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Formatter;

use function Flow\ETL\DSL\{bool_schema,
    date_schema,
    datetime_schema,
    empty_schema,
    enum_schema,
    float_schema,
    int_schema,
    json_schema,
    list_schema,
    map_schema,
    schema,
    str_schema,
    structure_schema,
    time_schema,
    uuid_schema,
    xml_element_schema,
    xml_schema};
use function Flow\Types\DSL\{type_integer, type_list, type_map, type_optional, type_string, type_structure};
use Flow\ETL\Schema\Formatter\PHPSchemaFormatter;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Unit\Schema\Formatter\Fixtures\StringEnum;

final class PHPSchemaFormatterTest extends FlowTestCase
{
    public function test_formatting_empty_schema() : void
    {
        self::assertEquals(
            '\\Flow\\ETL\\DSL\\schema();',
            (new PHPSchemaFormatter())->format(empty_schema())
        );
    }

    public function test_formatting_enum_schema() : void
    {
        self::assertEquals(
            <<<'PHP'
\Flow\ETL\DSL\schema(
    \Flow\ETL\DSL\enum_schema("enum", type: \Flow\ETL\Tests\Unit\Schema\Formatter\Fixtures\StringEnum::class, nullable: false, metadata: \Flow\ETL\DSL\schema_metadata()),
);
PHP,
            (new PHPSchemaFormatter())->format(schema(
                enum_schema('enum', type: StringEnum::class, nullable: false, metadata: null),
            ))
        );
    }

    public function test_formatting_float_schema() : void
    {
        self::assertEquals(
            <<<'PHP'
\Flow\ETL\DSL\schema(
    \Flow\ETL\DSL\float_schema("float", nullable: false, metadata: \Flow\ETL\DSL\schema_metadata()),
);
PHP,
            (new PHPSchemaFormatter())->format(schema(
                float_schema('float', nullable: false, metadata: null),
            ))
        );
    }

    public function test_formatting_list_schema() : void
    {
        self::assertEquals(
            <<<'PHP'
\Flow\ETL\DSL\schema(
    \Flow\ETL\DSL\list_schema("list", type: \Flow\Types\DSL\type_list(element: \Flow\Types\DSL\type_integer()), nullable: false, metadata: \Flow\ETL\DSL\schema_metadata()),
);
PHP,
            (new PHPSchemaFormatter())->format(schema(
                list_schema('list', type: type_list(type_integer())),
            ))
        );
    }

    public function test_formatting_map_schema() : void
    {
        self::assertEquals(
            <<<'PHP'
\Flow\ETL\DSL\schema(
    \Flow\ETL\DSL\map_schema("map", type: \Flow\Types\DSL\type_map(key_type: \Flow\Types\DSL\type_integer(), value_type: \Flow\Types\DSL\type_string()), nullable: false, metadata: \Flow\ETL\DSL\schema_metadata()),
);
PHP,
            (new PHPSchemaFormatter())->format(schema(
                map_schema('map', type_map(type_integer(), type_string()), nullable: false, metadata: null),
            ))
        );
    }

    public function test_formatting_simple_schema() : void
    {
        self::assertEquals(
            <<<PHP
\\Flow\\ETL\\DSL\\schema(
    \\Flow\\ETL\\DSL\\integer_schema("id", nullable: false, metadata: \\Flow\\ETL\\DSL\\schema_metadata()),
    \\Flow\\ETL\\DSL\\string_schema("name", nullable: true, metadata: \\Flow\\ETL\\DSL\\schema_metadata()),
    \\Flow\\ETL\\DSL\\bool_schema("active", nullable: false, metadata: \\Flow\\ETL\\DSL\\schema_metadata()),
    \\Flow\\ETL\\DSL\date_schema("created_at", nullable: true, metadata: \\Flow\\ETL\\DSL\\schema_metadata()),
    \\Flow\\ETL\\DSL\datetime_schema("updated_at", nullable: true, metadata: \\Flow\\ETL\\DSL\\schema_metadata()),
    \\Flow\\ETL\\DSL\\time_schema("deleted_at", nullable: true, metadata: \\Flow\\ETL\\DSL\\schema_metadata()),
    \\Flow\\ETL\\DSL\\json_schema("json", nullable: true, metadata: \\Flow\\ETL\\DSL\\schema_metadata()),
    \\Flow\\ETL\\DSL\\uuid_schema("uuid", nullable: true, metadata: \\Flow\\ETL\\DSL\\schema_metadata()),
    \\Flow\\ETL\\DSL\\xml_schema("xml", nullable: true, metadata: \\Flow\\ETL\\DSL\\schema_metadata()),
    \\Flow\\ETL\\DSL\\xml_element_schema("xml_element", nullable: true, metadata: \\Flow\\ETL\\DSL\\schema_metadata()),
);
PHP,
            (new PHPSchemaFormatter())->format(schema(
                int_schema('id', nullable: false),
                str_schema('name', nullable: true),
                bool_schema('active', false),
                date_schema('created_at', nullable: true),
                datetime_schema('updated_at', nullable: true),
                time_schema('deleted_at', nullable: true),
                json_schema('json', nullable: true),
                uuid_schema('uuid', nullable: true),
                xml_schema('xml', nullable: true),
                xml_element_schema('xml_element', nullable: true),
            ))
        );
    }

    public function test_formatting_simple_schema_with_metadata() : void
    {
        self::assertEquals(
            <<<'PHP'
\Flow\ETL\DSL\schema(
    \Flow\ETL\DSL\integer_schema("id", nullable: false, metadata: \Flow\ETL\DSL\schema_metadata(["int" => 1, "str" => "str", "bool" => true, "float" => 1.1, "empty_array" => [], "array_of_ints" => [1, 2, 3], "array_of_arrays" => [[1, 2], ["a", "b"]], "associative_array" => ["key" => "value"]])),
);
PHP,
            (new PHPSchemaFormatter())->format(schema(
                int_schema('id', nullable: false, metadata: Metadata::fromArray([
                    'int' => 1,
                    'str' => 'str',
                    'bool' => true,
                    'float' => 1.1,
                    'empty_array' => [],
                    'array_of_ints' => [1, 2, 3],
                    'array_of_arrays' => [
                        [1, 2],
                        ['a', 'b'],
                    ],
                    'associative_array' => [
                        'key' => 'value',
                    ],
                ])),
            ))
        );
    }

    public function test_formatting_structure_schema() : void
    {
        self::assertEquals(
            <<<'PHP'
\Flow\ETL\DSL\schema(
    \Flow\ETL\DSL\structure_schema("structure", type: \Flow\Types\DSL\type_structure(elements: ["int" => \Flow\Types\DSL\type_optional(\Flow\Types\DSL\type_integer()), "string" => \Flow\Types\DSL\type_optional(\Flow\Types\DSL\type_string())]), nullable: false, metadata: \Flow\ETL\DSL\schema_metadata()),
);
PHP,
            (new PHPSchemaFormatter())->format(schema(
                structure_schema('structure', type_structure(
                    ['int' => type_optional(type_integer()), 'string' => type_optional(type_string())],
                ))
            ))
        );
    }
}
