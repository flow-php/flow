<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ClassStringType;
use Flow\Types\Type\Logical\InstanceOfType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\LiteralType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\IntersectionType;
use Flow\Types\Type\Native\UnionType;

use function array_key_exists;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_callable;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_empty_array;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_non_empty_string;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_numeric_string;
use function Flow\Types\DSL\type_object;
use function Flow\Types\DSL\type_positive_integer;
use function Flow\Types\DSL\type_resource;
use function Flow\Types\DSL\type_scalar;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;
use function mb_strtolower;

final class TypeFactory
{
    /**
     * @param array<string, mixed> $data
     *
     * @return Type<mixed>
     */
    public static function fromArray(array $data): Type
    {
        type_array()->assert($data);

        if (!array_key_exists('type', $data)) {
            throw new InvalidArgumentException("Missing 'type' key in type definition");
        }

        $type = type_string()->assert($data['type']);

        return match ($type) {
            'float' => type_float(),
            'integer' => type_integer(),
            'positive_integer' => type_positive_integer(),
            'boolean' => type_boolean(),
            'string' => type_string(),
            'non_empty_string' => type_non_empty_string(),
            'callable' => type_callable(),
            'array' => type_array(),
            'empty_array' => type_empty_array(),
            'enum' => EnumType::fromArray($data),
            'null' => type_null(),
            'object' => type_object(),
            'instance_of' => InstanceOfType::fromArray($data),
            'class_string' => ClassStringType::fromArray($data),
            'resource' => type_resource(),
            'time' => type_time(),
            'timezone' => type_time_zone(),
            'date' => type_date(),
            'datetime' => type_datetime(),
            'json' => type_json(),
            'uuid' => type_uuid(),
            'literal' => LiteralType::fromArray($data),
            'list' => ListType::fromArray($data),
            'map' => MapType::fromArray($data),
            'structure' => StructureType::fromArray($data),
            'xml_element' => type_xml_element(),
            'xml' => type_xml(),
            'union' => UnionType::fromArray($data),
            'intersection' => IntersectionType::fromArray($data),
            'optional' => OptionalType::fromArray($data),
            'scalar' => type_scalar(),
            'mixed' => type_mixed(),
            'numeric-string' => type_numeric_string(),
            'html' => type_html(),
            'html_element' => type_html_element(),
            default => throw new InvalidArgumentException("Unknown type '" . $data['type'] . "'"),
        };
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return Type<mixed>
     */
    public static function fromString(string $name): Type
    {
        return match (mb_strtolower($name)) {
            'int', 'integer' => self::fromArray(['type' => 'integer', 'scalar_type' => 'integer']),
            'float', 'double', 'real' => self::fromArray(['type' => 'float', 'scalar_type' => 'float']),
            'string' => self::fromArray(['type' => 'string', 'scalar_type' => 'string']),
            'bool', 'boolean' => self::fromArray(['type' => 'boolean', 'scalar_type' => 'boolean']),
            default => self::fromArray(['type' => $name]),
        };
    }
}
