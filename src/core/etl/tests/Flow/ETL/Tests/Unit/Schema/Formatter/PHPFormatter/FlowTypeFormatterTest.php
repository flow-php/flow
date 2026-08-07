<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Formatter\PHPFormatter;

use Flow\ETL\Schema\Formatter\PHPFormatter\TypeFormatter;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_callable;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_resource;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;

final class FlowTypeFormatterTest extends FlowTestCase
{
    public function test_format_array_type(): void
    {
        static::assertEquals('\\Flow\\Types\\DSL\\type_array()', (new TypeFormatter())->format(type_array()));
    }

    public function test_format_list_type(): void
    {
        static::assertEquals(
            '\\Flow\\Types\\DSL\\type_list(element: \\Flow\\Types\\DSL\\type_integer())',
            (new TypeFormatter())->format(type_list(type_integer())),
        );
    }

    public function test_format_map_type(): void
    {
        static::assertEquals(
            '\\Flow\\Types\\DSL\\type_map(key_type: \\Flow\\Types\\DSL\\type_integer(), value_type: \\Flow\\Types\\DSL\\type_string())',
            (new TypeFormatter())->format(type_map(type_integer(), type_string())),
        );
    }

    public function test_format_structure_type(): void
    {
        static::assertEquals(
            '\\Flow\\Types\\DSL\\type_structure(elements: ["name" => \\Flow\\Types\\DSL\\type_string(), "age" => \\Flow\\Types\\DSL\\type_integer()])',
            (new TypeFormatter())->format(type_structure([
                'name' => type_string(),
                'age' => type_integer(),
            ])),
        );

        static::assertEquals(
            '\\Flow\\Types\\DSL\\type_structure(elements: ["name" => \\Flow\\Types\\DSL\\type_optional(\\Flow\\Types\\DSL\\type_string()), "age" => \\Flow\\Types\\DSL\\type_integer()])',
            (new TypeFormatter())->format(type_structure([
                'name' => type_optional(type_string()),
                'age' => type_integer(),
            ])),
        );
    }

    public function test_format_structure_type_with_optional_elements(): void
    {
        static::assertEquals(
            '\\Flow\\Types\\DSL\\type_structure(elements: ["name" => \\Flow\\Types\\DSL\\type_string()], optional_elements: ["nickname" => \\Flow\\Types\\DSL\\type_string()])',
            (new TypeFormatter())->format(type_structure(['name' => type_string()], ['nickname' => type_string()])),
        );
    }

    public function test_format_structure_type_with_allow_extra(): void
    {
        static::assertEquals(
            '\\Flow\\Types\\DSL\\type_structure(elements: ["name" => \\Flow\\Types\\DSL\\type_string()], allow_extra: true)',
            (new TypeFormatter())->format(type_structure(['name' => type_string()], [], true)),
        );
    }

    public function test_format_structure_type_with_optional_elements_and_allow_extra(): void
    {
        static::assertEquals(
            '\\Flow\\Types\\DSL\\type_structure(elements: ["name" => \\Flow\\Types\\DSL\\type_string()], optional_elements: ["nickname" => \\Flow\\Types\\DSL\\type_string()], allow_extra: true)',
            (new TypeFormatter())->format(type_structure(
                ['name' => type_string()],
                ['nickname' => type_string()],
                true,
            )),
        );
    }

    public function test_format_nullable_structure_type_with_optional_elements(): void
    {
        static::assertEquals(
            '\\Flow\\Types\\DSL\\type_optional(\\Flow\\Types\\DSL\\type_structure(elements: ["name" => \\Flow\\Types\\DSL\\type_string()], optional_elements: ["nickname" => \\Flow\\Types\\DSL\\type_string()]))',
            (new TypeFormatter())->format(type_optional(type_structure(['name' => type_string()], [
                'nickname' => type_string(),
            ]))),
        );
    }

    public function test_formatting_simple_types(): void
    {
        static::assertEquals('\\Flow\\Types\\DSL\\type_null()', (new TypeFormatter())->format(type_null()));
        static::assertEquals('\\Flow\\Types\\DSL\\type_string()', (new TypeFormatter())->format(type_string()));
        static::assertEquals('\\Flow\\Types\\DSL\\type_integer()', (new TypeFormatter())->format(type_integer()));
        static::assertEquals('\\Flow\\Types\\DSL\\type_float()', (new TypeFormatter())->format(type_float()));
        static::assertEquals('\\Flow\\Types\\DSL\\type_uuid()', (new TypeFormatter())->format(type_uuid()));
        static::assertEquals('\\Flow\\Types\\DSL\\type_boolean()', (new TypeFormatter())->format(type_boolean()));
        static::assertEquals('\\Flow\\Types\\DSL\\type_date()', (new TypeFormatter())->format(type_date()));
        static::assertEquals('\\Flow\\Types\\DSL\\type_datetime()', (new TypeFormatter())->format(type_datetime()));
        static::assertEquals('\\Flow\\Types\\DSL\\type_time()', (new TypeFormatter())->format(type_time()));
        static::assertEquals('\\Flow\\Types\\DSL\\type_html()', (new TypeFormatter())->format(type_html()));
        static::assertEquals(
            '\\Flow\\Types\\DSL\\type_html_element()',
            (new TypeFormatter())->format(type_html_element()),
        );
        static::assertEquals('\\Flow\\Types\\DSL\\type_xml()', (new TypeFormatter())->format(type_xml()));
        static::assertEquals(
            '\\Flow\\Types\\DSL\\type_xml_element()',
            (new TypeFormatter())->format(type_xml_element()),
        );
        static::assertEquals('\\Flow\\Types\\DSL\\type_resource()', (new TypeFormatter())->format(type_resource()));
        static::assertEquals('\\Flow\\Types\\DSL\\type_callable()', (new TypeFormatter())->format(type_callable()));
        static::assertEquals('\\Flow\\Types\\DSL\\type_json()', (new TypeFormatter())->format(type_json()));
    }
}
