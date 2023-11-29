<?php declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical\Map;

use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_string;
use Flow\ETL\PHP\Type\Native\ScalarType;

final class MapKey
{
    public function __construct(private readonly ScalarType $value)
    {
    }

    public static function fromType(ScalarType $type) : self
    {
        return new self($type);
    }

    public static function integer() : self
    {
        return new self(type_int(false));
    }

    public static function string() : self
    {
        return new self(type_string(false));
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

    public function type() : ScalarType
    {
        return $this->value;
    }
}
