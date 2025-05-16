<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use function Flow\Types\DSL\{
    type_array,
    type_boolean,
    type_callable,
    type_datetime,
    type_enum,
    type_float,
    type_from_array,
    type_instance_of,
    type_integer,
    type_json,
    type_list,
    type_map,
    type_null,
    type_resource,
    type_string,
    type_structure,
    type_time,
    type_xml,
    type_xml_element
};
use function Flow\Types\DSL\type_uuid;
use Flow\Types\Tests\Unit\Type\Fixtures\SomeEnum;
use PHPUnit\Framework\TestCase;

final class TypeFactoryTest extends TestCase
{
    public function test_normalizing_and_creating_all_scalar() : void
    {
        $string = type_string();
        self::assertEquals($string, type_from_array($string->normalize()));
        $integer = type_integer();
        self::assertEquals($integer, type_from_array($integer->normalize()));
        $boolean = type_boolean();
        self::assertEquals($boolean, type_from_array($boolean->normalize()));
        $float = type_float();
        self::assertEquals($float, type_from_array($float->normalize()));
    }

    public function test_normalizing_and_creating_array_type() : void
    {
        $array = type_array();
        self::assertEquals($array, type_from_array($array->normalize()));
    }

    public function test_normalizing_and_creating_callable_type() : void
    {
        $callable = type_callable();
        self::assertEquals($callable, type_from_array($callable->normalize()));
    }

    public function test_normalizing_and_creating_datetime_type() : void
    {
        $datetime = type_datetime();
        self::assertEquals($datetime, type_from_array($datetime->normalize()));
    }

    public function test_normalizing_and_creating_enum_type() : void
    {
        $enum = type_enum(SomeEnum::class);
        self::assertEquals($enum, type_from_array($enum->normalize()));
    }

    public function test_normalizing_and_creating_json_type() : void
    {
        $json = type_json();
        self::assertEquals($json, type_from_array($json->normalize()));
    }

    public function test_normalizing_and_creating_list_type() : void
    {
        $list = type_list(type_string());
        self::assertEquals($list, type_from_array($list->normalize()));
    }

    public function test_normalizing_and_creating_map_type() : void
    {
        $map = type_map(type_string(), type_integer());
        self::assertEquals($map, type_from_array($map->normalize()));
    }

    public function test_normalizing_and_creating_null_type() : void
    {
        $null = type_null();
        self::assertEquals($null, type_from_array($null->normalize()));
    }

    public function test_normalizing_and_creating_object_type() : void
    {
        $object = type_instance_of(\stdClass::class);
        self::assertEquals($object, type_from_array($object->normalize()));
    }

    public function test_normalizing_and_creating_resource_type() : void
    {
        $resource = type_resource();
        self::assertEquals($resource, type_from_array($resource->normalize()));
    }

    public function test_normalizing_and_creating_structure_type() : void
    {
        $structure = type_structure(
            [
                'name' => type_string(),
                'age' => type_integer(),
                'list' => type_list(type_string()),
                'map' => type_map(type_string(), type_integer()),
                'object' => type_instance_of(\stdClass::class),
            ]
        );

        self::assertEquals($structure, type_from_array($structure->normalize()));
    }

    public function test_normalizing_and_creating_uuid_type() : void
    {
        $uuid = type_uuid();
        self::assertEquals($uuid, type_from_array($uuid->normalize()));
    }

    public function test_normalizing_and_creating_xml_element_type() : void
    {
        $xmlElement = type_xml_element();
        self::assertEquals($xmlElement, type_from_array($xmlElement->normalize()));
    }

    public function test_normalizing_and_creating_xml_type() : void
    {
        $xml = type_xml();
        self::assertEquals($xml, type_from_array($xml->normalize()));
    }

    public function test_normalizing_date() : void
    {
        $date = type_datetime();
        self::assertEquals($date, type_from_array($date->normalize()));
    }

    public function test_normalizing_date_time() : void
    {
        $dateTime = type_datetime();
        self::assertEquals($dateTime, type_from_array($dateTime->normalize()));
    }

    public function test_normalizing_time() : void
    {
        $time = type_time();
        self::assertEquals($time, type_from_array($time->normalize()));
    }
}
