<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use function Flow\Types\DSL\{type_array,
    type_boolean,
    type_callable,
    type_date,
    type_datetime,
    type_float,
    type_integer,
    type_json,
    type_literal,
    type_mixed,
    type_non_empty_string,
    type_null,
    type_numeric_string,
    type_object,
    type_positive_integer,
    type_resource,
    type_scalar,
    type_string,
    type_time,
    type_uuid,
    type_xml,
    type_xml_element};
use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\{ClassStringType, InstanceOfType, ListType, MapType, OptionalType, StructureType};
use Flow\Types\Type\Native\{EnumType, IntersectionType, UnionType};

final class TypeFactory
{
    /**
     * @param array $data
     *
     * @return Type<mixed>
     */
    public static function fromArray(array $data) : Type
    {
        if (!\array_key_exists('type', $data)) {
            throw new InvalidArgumentException("Missing 'type' key in type definition");
        }

        return match ($data['type']) {
            'float' => type_float(),
            'integer' => type_integer(),
            'positive_integer' => type_positive_integer(),
            'boolean' => type_boolean(),
            'string' => type_string(),
            'non_empty_string' => type_non_empty_string(),
            'callable' => type_callable(),
            'array' => type_array(),
            /** @phpstan-ignore argument.type */
            'enum' => EnumType::fromArray($data),
            'null' => type_null(),
            'object' => type_object(),
            /** @phpstan-ignore argument.type */
            'instance_of' => InstanceOfType::fromArray($data),
            'class_string' => ClassStringType::fromArray($data),
            'resource' => type_resource(),
            'time' => type_time(),
            'date' => type_date(),
            'datetime' => type_datetime(),
            'json' => type_json(),
            'uuid' => type_uuid(),
            'literal' => self::createLiteralFromString($data['value']),
            /** @phpstan-ignore argument.type */
            'list' => ListType::fromArray($data),
            /** @phpstan-ignore argument.type */
            'map' => MapType::fromArray($data),
            /** @phpstan-ignore argument.type */
            'structure' => StructureType::fromArray($data),
            'xml_element' => type_xml_element(),
            'xml' => type_xml(),
            /** @phpstan-ignore argument.type */
            'union' => UnionType::fromArray($data),
            /** @phpstan-ignore argument.type */
            'intersection' => IntersectionType::fromArray($data),
            /** @phpstan-ignore argument.type */
            'optional' => OptionalType::fromArray($data),
            'scalar' => type_scalar(),
            'mixed' => type_mixed(),
            'numeric-string' => type_numeric_string(),
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

    /**
     * @return Type<mixed>
     */
    private static function createLiteralFromString(string $value) : Type
    {
        if ($value === 'true') {
            return type_literal(true);
        }

        if ($value === 'false') {
            return type_literal(false);
        }

        if (\is_numeric($value)) {
            if (\str_contains($value, '.')) {
                return type_literal((float) $value);
            }

            return type_literal((int) $value);
        }

        return type_literal($value);
    }
}
