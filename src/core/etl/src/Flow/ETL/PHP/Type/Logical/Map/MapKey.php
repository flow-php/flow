<?php declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical\Map;

use Flow\ETL\PHP\Type\Native\ScalarType;

final class MapKey
{
    private function __construct(private readonly ScalarType $value)
    {
    }

    public static function fromType(ScalarType $type) : self
    {
        return new self($type);
    }

    public static function integer() : self
    {
        return new self(ScalarType::integer());
    }

    public static function string() : self
    {
        return new self(ScalarType::string());
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

    public function value() : ScalarType
    {
        return $this->value;
    }
}
