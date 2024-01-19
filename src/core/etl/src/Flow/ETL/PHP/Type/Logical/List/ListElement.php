<?php declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical\List;

use function Flow\ETL\DSL\type_boolean;
use function Flow\ETL\DSL\type_datetime;
use function Flow\ETL\DSL\type_float;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_json;
use function Flow\ETL\DSL\type_object;
use function Flow\ETL\DSL\type_string;
use function Flow\ETL\DSL\type_uuid;
use function Flow\ETL\DSL\type_xml;
use function Flow\ETL\DSL\type_xml_node;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Type;

final class ListElement
{
    public function __construct(private readonly Type $value)
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

    public static function xml_node(bool $nullable = false) : self
    {
        return new self(type_xml_node($nullable));
    }

    public function isEqual(mixed $value) : bool
    {
        return $this->value->isEqual($value);
    }

    public function isValid(mixed $value) : bool
    {
        return $this->value->isValid($value);
    }

    public function toString() : string
    {
        return $this->value->toString();
    }

    public function type() : Type
    {
        return $this->value;
    }
}
