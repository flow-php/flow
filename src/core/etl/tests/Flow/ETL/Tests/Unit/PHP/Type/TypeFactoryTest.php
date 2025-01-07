<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type;

use function Flow\ETL\DSL\{
    type_array,
    type_boolean,
    type_callable,
    type_datetime,
    type_enum,
    type_float,
    type_integer,
    type_json,
    type_list,
    type_map,
    type_null,
    type_object,
    type_resource,
    type_string,
    type_structure,
    type_time,
    type_uuid,
    type_xml,
    type_xml_element};
use Flow\ETL\PHP\Type\TypeFactory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Unit\PHP\Type\Fixtures\SomeEnum;

final class TypeFactoryTest extends FlowTestCase
{
    public function test_normalizing_and_creating_all_scalar() : void
    {
        $string = type_string();
        self::assertEquals($string, TypeFactory::fromArray($string->normalize()));
        $integer = type_integer();
        self::assertEquals($integer, TypeFactory::fromArray($integer->normalize()));
        $boolean = type_boolean();
        self::assertEquals($boolean, TypeFactory::fromArray($boolean->normalize()));
        $float = type_float();
        self::assertEquals($float, TypeFactory::fromArray($float->normalize()));
    }

    public function test_normalizing_and_creating_array_type() : void
    {
        $array = type_array();
        self::assertEquals($array, TypeFactory::fromArray($array->normalize()));
    }

    public function test_normalizing_and_creating_callable_type() : void
    {
        $callable = type_callable();
        self::assertEquals($callable, TypeFactory::fromArray($callable->normalize()));
    }

    public function test_normalizing_and_creating_datetime_type() : void
    {
        $datetime = type_datetime();
        self::assertEquals($datetime, TypeFactory::fromArray($datetime->normalize()));
    }

    public function test_normalizing_and_creating_enum_type() : void
    {
        $enum = type_enum(SomeEnum::class);
        self::assertEquals($enum, TypeFactory::fromArray($enum->normalize()));
    }

    public function test_normalizing_and_creating_json_type() : void
    {
        $json = type_json();
        self::assertEquals($json, TypeFactory::fromArray($json->normalize()));
    }

    public function test_normalizing_and_creating_list_type() : void
    {
        $list = type_list(type_string());
        self::assertEquals($list, TypeFactory::fromArray($list->normalize()));
    }

    public function test_normalizing_and_creating_map_type() : void
    {
        $map = type_map(type_string(), type_integer());
        self::assertEquals($map, TypeFactory::fromArray($map->normalize()));
    }

    public function test_normalizing_and_creating_null_type() : void
    {
        $null = type_null();
        self::assertEquals($null, TypeFactory::fromArray($null->normalize()));
    }

    public function test_normalizing_and_creating_object_type() : void
    {
        $object = type_object(\stdClass::class);
        self::assertEquals($object, TypeFactory::fromArray($object->normalize()));
    }

    public function test_normalizing_and_creating_resource_type() : void
    {
        $resource = type_resource();
        self::assertEquals($resource, TypeFactory::fromArray($resource->normalize()));
    }

    public function test_normalizing_and_creating_structure_type() : void
    {
        $structure = type_structure(
            [
                'name' => type_string(),
                'age' => type_integer(),
                'list' => type_list(type_string()),
                'map' => type_map(type_string(), type_integer()),
                'object' => type_object(\stdClass::class),
            ]
        );

        self::assertEquals($structure, TypeFactory::fromArray($structure->normalize()));
    }

    public function test_normalizing_and_creating_uuid_type() : void
    {
        $uuid = type_uuid();
        self::assertEquals($uuid, TypeFactory::fromArray($uuid->normalize()));
    }

    public function test_normalizing_and_creating_xml_element_type() : void
    {
        $xmlElement = type_xml_element();
        self::assertEquals($xmlElement, TypeFactory::fromArray($xmlElement->normalize()));
    }

    public function test_normalizing_and_creating_xml_type() : void
    {
        $xml = type_xml();
        self::assertEquals($xml, TypeFactory::fromArray($xml->normalize()));
    }

    public function test_normalizing_date() : void
    {
        $date = type_datetime();
        self::assertEquals($date, TypeFactory::fromArray($date->normalize()));
    }

    public function test_normalizing_date_time() : void
    {
        $dateTime = type_datetime();
        self::assertEquals($dateTime, TypeFactory::fromArray($dateTime->normalize()));
    }

    public function test_normalizing_time() : void
    {
        $time = type_time();
        self::assertEquals($time, TypeFactory::fromArray($time->normalize()));
    }
}
