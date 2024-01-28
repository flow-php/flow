<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\DateTimeType;
use Flow\ETL\PHP\Type\Logical\JsonType;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Logical\UuidType;
use Flow\ETL\PHP\Type\Logical\XMLNodeType;
use Flow\ETL\PHP\Type\Logical\XMLType;
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\Native\CallableType;
use Flow\ETL\PHP\Type\Native\EnumType;
use Flow\ETL\PHP\Type\Native\NullType;
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\PHP\Type\Native\ResourceType;
use Flow\ETL\PHP\Type\Native\ScalarType;

final class TypeFactory
{
    public static function fromArray(array $data) : Type
    {
        if (!\array_key_exists('type', $data)) {
            throw new \InvalidArgumentException("Missing 'type' key in type definition");
        }

        return match ($data['type']) {
            'scalar' => ScalarType::fromArray($data),
            'callable' => CallableType::fromArray($data),
            'array' => ArrayType::fromArray($data),
            'enum' => EnumType::fromArray($data),
            'null' => NullType::fromArray($data),
            'object' => ObjectType::fromArray($data),
            'resource' => ResourceType::fromArray($data),
            'datetime' => DateTimeType::fromArray($data),
            'json' => JsonType::fromArray($data),
            'uuid' => UuidType::fromArray($data),
            'list' => ListType::fromArray($data),
            'map' => MapType::fromArray($data),
            'structure' => StructureType::fromArray($data),
            'xml_node' => XMLNodeType::fromArray($data),
            'xml' => XMLType::fromArray($data),
            default => throw new InvalidArgumentException("Unknown type '{$data['type']}'"),
        };
    }
}
