<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical\List;

use function Flow\ETL\DSL\{type_boolean,
    type_datetime,
    type_float,
    type_int,
    type_json,
    type_object,
    type_string,
    type_uuid,
    type_xml,
    type_xml_element};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\{ListType, MapType, StructureType};
use Flow\ETL\PHP\Type\{Type, TypeFactory};

final class ListElement
{
    /**
     * @param Type<mixed> $type
     */
    public function __construct(private readonly Type $type)
    {
    }

    public static function boolean() : self
    {
        return new self(type_boolean(false));
    }

    public static function datetime(bool $nullable = false) : self
    {
        return new self(type_datetime($nullable));
    }

    public static function float() : self
    {
        return new self(type_float(false));
    }

    public static function fromArray(array $data) : self
    {
        if (!\array_key_exists('type', $data)) {
            throw new InvalidArgumentException("Missing 'type' key in list element definition");
        }

        return new self(TypeFactory::fromArray($data['type']));
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
        return new self(type_int(false));
    }

    public static function json(bool $nullable = false) : self
    {
        return new self(type_json($nullable));
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
    public static function object(string $class, bool $nullable = false) : self
    {
        if (\is_a($class, \DateTimeInterface::class, true)) {
            return new self(type_datetime($nullable));
        }

        return new self(type_object($class, $nullable));
    }

    public static function string() : self
    {
        return new self(type_string(false));
    }

    public static function structure(StructureType $structure) : self
    {
        return new self($structure);
    }

    public static function uuid(bool $nullable = false) : self
    {
        return new self(type_uuid($nullable));
    }

    public static function xml(bool $nullable = false) : self
    {
        return new self(type_xml($nullable));
    }

    public static function xml_element(bool $nullable = false) : self
    {
        return new self(type_xml_element($nullable));
    }

    public function isEqual(mixed $value) : bool
    {
        return $this->type->isEqual($value);
    }

    public function isValid(mixed $value) : bool
    {
        return $this->type->isValid($value);
    }

    public function normalize() : array
    {
        return [
            'type' => $this->type->normalize(),
        ];
    }

    public function toString() : string
    {
        return $this->type->toString();
    }

    /**
     * @return Type<mixed>
     */
    public function type() : Type
    {
        return $this->type;
    }
}
