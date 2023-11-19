<?php declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical\Map;

use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\PHP\Type\Type;

final class MapValue
{
    private function __construct(private readonly Type $value)
    {
    }

    public static function boolean(bool $nullable = false) : self
    {
        return new self(ScalarType::boolean($nullable));
    }

    public static function float(bool $nullable = false) : self
    {
        return new self(ScalarType::float($nullable));
    }

    public static function fromType(Type $type) : self
    {
        return new self($type);
    }

    public static function integer(bool $nullable = false) : self
    {
        return new self(ScalarType::integer($nullable));
    }

    public static function integer32(bool $nullable = false) : self
    {
        return new self(ScalarType::integer32($nullable));
    }

    public static function integer64(bool $nullable = false) : self
    {
        return new self(ScalarType::integer64($nullable));
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
        return new self(ObjectType::of($class, $optional));
    }

    public static function string(bool $nullable = false) : self
    {
        return new self(ScalarType::string($nullable));
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
