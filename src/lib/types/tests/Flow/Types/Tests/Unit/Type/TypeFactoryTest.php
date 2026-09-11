<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use Flow\Types\Tests\Unit\Type\Fixtures\SomeEnum;
use Flow\Types\Type\TypeFactory;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use stdClass;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_callable;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_empty_array;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_resource;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;

final class TypeFactoryTest extends TestCase
{
    public function test_normalizing_and_creating_all_scalar(): void
    {
        $string = type_string();
        static::assertEquals($string, type_from_array($string->normalize()));
        $integer = type_integer();
        static::assertEquals($integer, type_from_array($integer->normalize()));
        $boolean = type_boolean();
        static::assertEquals($boolean, type_from_array($boolean->normalize()));
        $float = type_float();
        static::assertEquals($float, type_from_array($float->normalize()));
    }

    public function test_normalizing_and_creating_array_type(): void
    {
        $array = type_array();
        static::assertEquals($array, type_from_array($array->normalize()));
    }

    public function test_normalizing_and_creating_callable_type(): void
    {
        $callable = type_callable();
        static::assertEquals($callable, type_from_array($callable->normalize()));
    }

    public function test_normalizing_and_creating_datetime_type(): void
    {
        $datetime = type_datetime();
        static::assertEquals($datetime, type_from_array($datetime->normalize()));
    }

    public function test_normalizing_and_creating_empty_array_type(): void
    {
        $emptyArray = type_empty_array();
        static::assertEquals($emptyArray, type_from_array($emptyArray->normalize()));
        static::assertEquals($emptyArray, TypeFactory::fromString('empty_array'));
    }

    #[TestWith(['double'])]
    #[TestWith(['real'])]
    public function test_double_and_real_resolve_to_float(string $alias): void
    {
        static::assertEquals(type_float(), TypeFactory::fromString($alias));
    }

    public function test_normalizing_and_creating_enum_type(): void
    {
        $enum = type_enum(SomeEnum::class);
        static::assertEquals($enum, type_from_array($enum->normalize()));
    }

    public function test_normalizing_and_creating_html_element_type(): void
    {
        $htmlElement = type_html_element();
        static::assertEquals($htmlElement, type_from_array($htmlElement->normalize()));
    }

    public function test_normalizing_and_creating_html_type(): void
    {
        $html = type_html();
        static::assertEquals($html, type_from_array($html->normalize()));
    }

    public function test_normalizing_and_creating_json_type(): void
    {
        $json = type_json();
        static::assertEquals($json, type_from_array($json->normalize()));
    }

    public function test_normalizing_and_creating_list_type(): void
    {
        $list = type_list(type_string());
        static::assertEquals($list, type_from_array($list->normalize()));
    }

    public function test_normalizing_and_creating_map_type(): void
    {
        $map = type_map(type_string(), type_integer());
        static::assertEquals($map, type_from_array($map->normalize()));
    }

    public function test_normalizing_and_creating_null_type(): void
    {
        $null = type_null();
        static::assertEquals($null, type_from_array($null->normalize()));
    }

    public function test_normalizing_and_creating_object_type(): void
    {
        $object = type_instance_of(stdClass::class);
        static::assertEquals($object, type_from_array($object->normalize()));
    }

    public function test_normalizing_and_creating_resource_type(): void
    {
        $resource = type_resource();
        static::assertEquals($resource, type_from_array($resource->normalize()));
    }

    public function test_normalizing_and_creating_structure_type(): void
    {
        $structure = type_structure([
            'name' => type_string(),
            'age' => type_integer(),
            'list' => type_list(type_string()),
            'map' => type_map(type_string(), type_integer()),
            'object' => type_instance_of(stdClass::class),
        ]);

        static::assertEquals($structure, type_from_array($structure->normalize()));
    }

    public function test_normalizing_and_creating_uuid_type(): void
    {
        $uuid = type_uuid();
        static::assertEquals($uuid, type_from_array($uuid->normalize()));
    }

    public function test_normalizing_and_creating_xml_element_type(): void
    {
        $xmlElement = type_xml_element();
        static::assertEquals($xmlElement, type_from_array($xmlElement->normalize()));
    }

    public function test_normalizing_and_creating_xml_type(): void
    {
        $xml = type_xml();
        static::assertEquals($xml, type_from_array($xml->normalize()));
    }

    public function test_normalizing_date(): void
    {
        $date = type_datetime();
        static::assertEquals($date, type_from_array($date->normalize()));
    }

    public function test_normalizing_date_time(): void
    {
        $dateTime = type_datetime();
        static::assertEquals($dateTime, type_from_array($dateTime->normalize()));
    }

    public function test_normalizing_time(): void
    {
        $time = type_time();
        static::assertEquals($time, type_from_array($time->normalize()));
    }

    public function test_normalizing_time_zone(): void
    {
        $timeZone = type_time_zone();
        static::assertEquals($timeZone, type_from_array($timeZone->normalize()));
    }
}
