<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema\Formatter\PHPFormatter;

use function Flow\ETL\DSL\{type_array, type_boolean, type_callable, type_date, type_datetime, type_float, type_int, type_json, type_list, type_map, type_null, type_resource, type_string, type_structure, type_time, type_uuid, type_xml, type_xml_element};
use Flow\ETL\Schema\Formatter\PHPFormatter\TypeFormatter;
use Flow\ETL\Tests\FlowTestCase;

final class FlowTypeFormatterTest extends FlowTestCase
{
    public function test_format_array_type() : void
    {
        self::assertEquals('\\Flow\\ETL\\DSL\\type_array(empty: false, nullable: false)', (new TypeFormatter())->format(type_array()));
        self::assertEquals('\\Flow\\ETL\\DSL\\type_array(empty: true, nullable: true)', (new TypeFormatter())->format(type_array(true, true)));
    }

    public function test_format_list_type() : void
    {
        self::assertEquals('\\Flow\\ETL\\DSL\\type_list(element: \\Flow\\ETL\\DSL\\type_integer(nullable: false), nullable: false)', (new TypeFormatter())->format(type_list(type_int())));
        self::assertEquals('\\Flow\\ETL\\DSL\\type_list(element: \\Flow\\ETL\\DSL\\type_integer(nullable: true), nullable: true)', (new TypeFormatter())->format(type_list(type_int(true), true)));
    }

    public function test_format_map_type() : void
    {
        self::assertEquals('\\Flow\\ETL\\DSL\\type_map(key_type: \\Flow\\ETL\\DSL\\type_integer(nullable: false), value_type: \\Flow\\ETL\\DSL\\type_string(nullable: false), nullable: false)', (new TypeFormatter())->format(type_map(type_int(), type_string())));
        self::assertEquals('\\Flow\\ETL\\DSL\\type_map(key_type: \\Flow\\ETL\\DSL\\type_integer(nullable: false), value_type: \\Flow\\ETL\\DSL\\type_string(nullable: true), nullable: true)', (new TypeFormatter())->format(type_map(type_int(), type_string(true), true)));
    }

    public function test_format_structure_type() : void
    {
        self::assertEquals(
            '\\Flow\\ETL\\DSL\\type_structure(elements: ["name" => \\Flow\\ETL\\DSL\\type_string(nullable: false), "age" => \\Flow\\ETL\\DSL\\type_integer(nullable: false)], nullable: false)',
            (new TypeFormatter())->format(
                type_structure(
                    [
                        'name' => type_string(),
                        'age' => type_int(),
                    ],
                    nullable: false
                )
            )
        );

        self::assertEquals(
            '\\Flow\\ETL\\DSL\\type_structure(elements: ["name" => \\Flow\\ETL\\DSL\\type_string(nullable: true), "age" => \\Flow\\ETL\\DSL\\type_integer(nullable: true)], nullable: true)',
            (new TypeFormatter())->format(
                type_structure(
                    [
                        'name' => type_string(true),
                        'age' => type_int(true),
                    ],
                    nullable: true
                )
            )
        );
    }

    public function test_formatting_simple_types() : void
    {
        self::assertEquals('\\Flow\\ETL\\DSL\\type_null()', (new TypeFormatter())->format(type_null()));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_string(nullable: false)', (new TypeFormatter())->format(type_string()));
        self::assertEquals('\\Flow\\ETL\\DSL\\type_string(nullable: true)', (new TypeFormatter())->format(type_string(true)));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_integer(nullable: false)', (new TypeFormatter())->format(type_int()));
        self::assertEquals('\\Flow\\ETL\\DSL\\type_integer(nullable: true)', (new TypeFormatter())->format(type_int(true)));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_float(nullable: false)', (new TypeFormatter())->format(type_float()));
        self::assertEquals('\\Flow\\ETL\\DSL\\type_float(nullable: true)', (new TypeFormatter())->format(type_float(true)));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_uuid(nullable: false)', (new TypeFormatter())->format(type_uuid()));
        self::assertEquals('\\Flow\\ETL\\DSL\\type_uuid(nullable: true)', (new TypeFormatter())->format(type_uuid(true)));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_boolean(nullable: false)', (new TypeFormatter())->format(type_boolean()));
        self::assertEquals('\\Flow\\ETL\\DSL\\type_boolean(nullable: true)', (new TypeFormatter())->format(type_boolean(true)));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_date(nullable: false)', (new TypeFormatter())->format(type_date()));
        self::assertEquals('\\Flow\\ETL\\DSL\\type_date(nullable: true)', (new TypeFormatter())->format(type_date(true)));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_datetime(nullable: false)', (new TypeFormatter())->format(type_datetime()));
        self::assertEquals('\\Flow\\ETL\\DSL\\type_datetime(nullable: true)', (new TypeFormatter())->format(type_datetime(true)));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_time(nullable: false)', (new TypeFormatter())->format(type_time()));
        self::assertEquals('\\Flow\\ETL\\DSL\\type_time(nullable: true)', (new TypeFormatter())->format(type_time(true)));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_xml(nullable: false)', (new TypeFormatter())->format(type_xml()));
        self::assertEquals('\\Flow\\ETL\\DSL\\type_xml(nullable: true)', (new TypeFormatter())->format(type_xml(true)));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_xml_element(nullable: false)', (new TypeFormatter())->format(type_xml_element()));
        self::assertEquals('\\Flow\\ETL\\DSL\\type_xml_element(nullable: true)', (new TypeFormatter())->format(type_xml_element(true)));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_resource(nullable: false)', (new TypeFormatter())->format(type_resource()));
        self::assertEquals('\\Flow\\ETL\\DSL\\type_resource(nullable: true)', (new TypeFormatter())->format(type_resource(true)));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_callable(nullable: false)', (new TypeFormatter())->format(type_callable()));
        self::assertEquals('\\Flow\\ETL\\DSL\\type_callable(nullable: true)', (new TypeFormatter())->format(type_callable(true)));

        self::assertEquals('\\Flow\\ETL\\DSL\\type_json(nullable: false)', (new TypeFormatter())->format(type_json()));
        self::assertEquals('\\Flow\\ETL\\DSL\\type_json(nullable: true)', (new TypeFormatter())->format(type_json(true)));
    }
}
