<?php declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical\Map;

use function Flow\ETL\DSL\type_boolean;
use function Flow\ETL\DSL\type_float;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_object;
use function Flow\ETL\DSL\type_string;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Type;

final class MapValue
{
    public function __construct(private readonly Type $value)
    {
    }

    public static function boolean() : self
    {
        return new self(type_boolean());
    }

    public static function float() : self
    {
        return new self(type_float());
    }

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
