<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

use function Flow\ETL\DSL\{type_array,
    type_boolean,
    type_callable,
    type_date,
    type_datetime,
    type_float,
    type_integer,
    type_json,
    type_null,
    type_resource,
    type_string,
    type_time,
    type_uuid,
    type_xml,
    type_xml_element};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\{ListType, MapType, OptionalType, StructureType};
use Flow\ETL\PHP\Type\Native\{
    EnumType,
    ObjectType,
    UnionType};

final class TypeFactory
{
    /**
     * @return Type<mixed>
     */
    public static function fromArray(array $data) : Type
    {
        if (!\array_key_exists('type', $data)) {
            throw new \InvalidArgumentException("Missing 'type' key in type definition");
        }

        return match ($data['type']) {
            'float' => type_float(),
            'integer' => type_integer(),
            'boolean' => type_boolean(),
            'string' => type_string(),
            'callable' => type_callable(),
            'array' => type_array(),
            'enum' => EnumType::fromArray($data),
            'null' => type_null(),
            'object' => ObjectType::fromArray($data),
            'resource' => type_resource(),
            'time' => type_time(),
            'date' => type_date(),
            'datetime' => type_datetime(),
            'json' => type_json(),
            'uuid' => type_uuid(),
            'list' => ListType::fromArray($data),
            'map' => MapType::fromArray($data),
            'structure' => StructureType::fromArray($data),
            'xml_element' => type_xml_element(),
            'xml' => type_xml(),
            'union' => UnionType::fromArray($data),
            'optional' => OptionalType::fromArray($data),
            default => throw new InvalidArgumentException("Unknown type '{$data['type']}'"),
        };
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return Type<mixed>
     */
    public static function fromString(string $name) : Type
    {
        return match (\mb_strtolower($name)) {
            'int','integer' => self::fromArray(['type' => 'integer', 'scalar_type' => 'integer']),
            'float' => self::fromArray(['type' => 'float', 'scalar_type' => 'float']),
            'string' => self::fromArray(['type' => 'string', 'scalar_type' => 'string']),
            'bool','boolean' => self::fromArray(['type' => 'boolean', 'scalar_type' => 'boolean']),
            default => self::fromArray(['type' => $name]),
        };
    }
}
