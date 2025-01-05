<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical\Map;

use function Flow\ETL\DSL\{type_boolean,
    type_datetime,
    type_float,
    type_int,
    type_object,
    type_string,
    type_xml,
    type_xml_element};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\{ListType, MapType};
use Flow\ETL\PHP\Type\{Type, TypeFactory};

final class MapValue
{
    /**
     * @param Type<mixed> $value
     */
    public function __construct(private readonly Type $value)
    {
    }

    public static function boolean() : self
    {
        return new self(type_boolean());
    }

    public static function datetime() : self
    {
        return new self(type_datetime());
    }

    public static function float() : self
    {
        return new self(type_float());
    }

    public static function fromArray(array $value) : self
    {
        if (!\array_key_exists('type', $value)) {
            throw new InvalidArgumentException('Missing "type" key in ' . self::class . ' fromArray()');
        }

        return new self(TypeFactory::fromArray($value['type']));
    }

    /**
     * @param Type<mixed> $type
     */
    public static function fromType(Type $type) : self
    {
        return new self($type);
    }

    public static function integer() : self
    {
        return new self(type_int());
    }

    public static function list(ListType $type) : self
    {
        return new self($type);
    }

    public static function map(MapType $type) : self
    {
        return new self($type);
    }

    /**
     * @param class-string $class
     */
    public static function object(string $class, bool $optional = false) : self
    {
        return new self(type_object($class, $optional));
    }

    public static function string() : self
    {
        return new self(type_string());
    }

    public static function uuid() : self
    {
        return new self(type_string());
    }

    public static function xml() : self
    {
        return new self(type_xml());
    }

    public static function xmlElement() : self
    {
        return new self(type_xml_element());
    }

    public function isEqual(mixed $value) : bool
    {
        return $this->value->isEqual($value);
    }

    public function isValid(mixed $value) : bool
    {
        return $this->value->isValid($value);
    }

    public function normalize() : array
    {
        return [
            'type' => $this->value->normalize(),
        ];
    }

    public function toString() : string
    {
        return $this->value->toString();
    }

    /**
     * @return Type<mixed>
     */
    public function type() : Type
    {
        return $this->value;
    }
}
