<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Formatter\PHPFormatter;

use function Flow\ETL\DSL\{type_array,
    type_boolean,
    type_callable,
    type_date,
    type_datetime,
    type_float,
    type_int,
    type_json,
    type_list,
    type_map,
    type_null,
    type_optional,
    type_resource,
    type_string,
    type_structure,
    type_time,
    type_uuid,
    type_xml,
    type_xml_element};
use Flow\ETL\Schema\Formatter\PHPFormatter\TypeFormatter;
use Flow\ETL\Tests\FlowTestCase;

final class FlowTypeFormatterTest extends FlowTestCase
{
    public function test_format_array_type() : void
    {
        self::assertEquals('\\Flow\\ETL\\DSL\\type_array()', (new TypeFormatter())->format(type_array()));
    }

    public function test_format_list_type() : void
    {
        self::assertEquals('\\Flow\\ETL\\DSL\\type_list(element: \\Flow\\ETL\\DSL\\type_integer())', (new TypeFormatter())->format(type_list(type_int())));
    }

    public function test_format_map_type() : void
    {
        self::assertEquals('\\Flow\\ETL\\DSL\\type_map(key_type: \\Flow\\ETL\\DSL\\type_integer(), value_type: \\Flow\\ETL\\DSL\\type_string())', (new TypeFormatter())->format(type_map(type_int(), type_string())));
    }

    public function test_format_structure_type() : void
    {
        self::assertEquals(
            '\\Flow\\ETL\\DSL\\type_structure(elements: ["name" => \\Flow\\ETL\\DSL\\type_string(), "age" => \\Flow\\ETL\\DSL\\type_integer()])',
            (new TypeFormatter())->format(
                type_structure(
                    [
                        'name' => type_string(),
                        'age' => type_int(),
                    ],
                )
            )
        );

        self::assertEquals(
            '\\Flow\\ETL\\DSL\\type_structure(elements: ["name" => \\Flow\\ETL\\DSL\\type_optional(\\Flow\\ETL\\DSL\\type_string()), "age" => \\Flow\\ETL\\DSL\\type_integer()])',
            (new TypeFormatter())->format(
                type_structure(
                    [
                        'name' => type_optional(type_string()),
                        'age' => type_int(),
                    ],
                )
            )
        );
    }

    public function test_formatting_simple_types() : void
    {
        self::assertEquals('\\Flow\\ETL\\DSL\\type_null()', (new TypeFormatter())->format(type_null()));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_string()', (new TypeFormatter())->format(type_string()));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_integer()', (new TypeFormatter())->format(type_int()));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_float()', (new TypeFormatter())->format(type_float()));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_uuid()', (new TypeFormatter())->format(type_uuid()));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_boolean()', (new TypeFormatter())->format(type_boolean()));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_date()', (new TypeFormatter())->format(type_date()));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_datetime()', (new TypeFormatter())->format(type_datetime()));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_time()', (new TypeFormatter())->format(type_time()));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_xml()', (new TypeFormatter())->format(type_xml()));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_xml_element()', (new TypeFormatter())->format(type_xml_element()));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_resource()', (new TypeFormatter())->format(type_resource()));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_callable()', (new TypeFormatter())->format(type_callable()));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_json()', (new TypeFormatter())->format(type_json()));
    }
}
