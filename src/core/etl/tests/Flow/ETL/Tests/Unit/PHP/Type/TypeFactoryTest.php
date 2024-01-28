<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type;

use function Flow\ETL\DSL\structure_element;
use function Flow\ETL\DSL\type_array;
use function Flow\ETL\DSL\type_boolean;
use function Flow\ETL\DSL\type_callable;
use function Flow\ETL\DSL\type_datetime;
use function Flow\ETL\DSL\type_enum;
use function Flow\ETL\DSL\type_float;
use function Flow\ETL\DSL\type_integer;
use function Flow\ETL\DSL\type_json;
use function Flow\ETL\DSL\type_list;
use function Flow\ETL\DSL\type_map;
use function Flow\ETL\DSL\type_null;
use function Flow\ETL\DSL\type_object;
use function Flow\ETL\DSL\type_resource;
use function Flow\ETL\DSL\type_string;
use function Flow\ETL\DSL\type_structure;
use function Flow\ETL\DSL\type_uuid;
use function Flow\ETL\DSL\type_xml;
use function Flow\ETL\DSL\type_xml_node;
use Flow\ETL\PHP\Type\TypeFactory;
use Flow\ETL\Tests\Unit\PHP\Type\Fixtures\SomeEnum;
use PHPUnit\Framework\TestCase;

final class TypeFactoryTest extends TestCase
{
    public function test_normalizing_and_creating_all_scalar() : void
    {
        $string = type_string();
        $this->assertEquals($string, TypeFactory::fromArray($string->normalize()));
        $integer = type_integer();
        $this->assertEquals($integer, TypeFactory::fromArray($integer->normalize()));
        $boolean = type_boolean();
        $this->assertEquals($boolean, TypeFactory::fromArray($boolean->normalize()));
        $float = type_float();
        $this->assertEquals($float, TypeFactory::fromArray($float->normalize()));
    }

    public function test_normalizing_and_creating_array_type() : void
    {
        $array = type_array();
        $this->assertEquals($array, TypeFactory::fromArray($array->normalize()));
    }

    public function test_normalizing_and_creating_callable_type() : void
    {
        $callable = type_callable();
        $this->assertEquals($callable, TypeFactory::fromArray($callable->normalize()));
    }

    public function test_normalizing_and_creating_datetime_type() : void
    {
        $datetime = type_datetime();
        $this->assertEquals($datetime, TypeFactory::fromArray($datetime->normalize()));
    }

    public function test_normalizing_and_creating_enum_type() : void
    {
        $enum = type_enum(SomeEnum::class);
        $this->assertEquals($enum, TypeFactory::fromArray($enum->normalize()));
    }

    public function test_normalizing_and_creating_json_type() : void
    {
        $json = type_json();
        $this->assertEquals($json, TypeFactory::fromArray($json->normalize()));
    }

    public function test_normalizing_and_creating_list_type() : void
    {
        $list = type_list(type_string());
        $this->assertEquals($list, TypeFactory::fromArray($list->normalize()));
    }

    public function test_normalizing_and_creating_map_type() : void
    {
        $map = type_map(type_string(), type_integer());
        $this->assertEquals($map, TypeFactory::fromArray($map->normalize()));
    }

    public function test_normalizing_and_creating_null_type() : void
    {
        $null = type_null();
        $this->assertEquals($null, TypeFactory::fromArray($null->normalize()));
    }

    public function test_normalizing_and_creating_object_type() : void
    {
        $object = type_object(\stdClass::class);
        $this->assertEquals($object, TypeFactory::fromArray($object->normalize()));
    }

    public function test_normalizing_and_creating_resource_type() : void
    {
        $resource = type_resource();
        $this->assertEquals($resource, TypeFactory::fromArray($resource->normalize()));
    }

    public function test_normalizing_and_creating_structure_type() : void
    {
        $structure = type_structure(
            [
                structure_element('name', type_string()),
                structure_element('age', type_integer()),
                structure_element('list', type_list(type_string())),
                structure_element('map', type_map(type_string(), type_integer())),
                structure_element('object', type_object(\stdClass::class)),
            ]
        );

        $this->assertEquals($structure, TypeFactory::fromArray($structure->normalize()));
    }

    public function test_normalizing_and_creating_uuid_type() : void
    {
        $uuid = type_uuid();
        $this->assertEquals($uuid, TypeFactory::fromArray($uuid->normalize()));
    }

    public function test_normalizing_and_creating_xml_node_type() : void
    {
        $xmlNode = type_xml_node();
        $this->assertEquals($xmlNode, TypeFactory::fromArray($xmlNode->normalize()));
    }

    public function test_normalizing_and_creating_xml_type() : void
    {
        $xml = type_xml();
        $this->assertEquals($xml, TypeFactory::fromArray($xml->normalize()));
    }
}
