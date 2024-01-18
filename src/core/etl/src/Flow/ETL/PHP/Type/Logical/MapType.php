<?php declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical;

use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Type;

final class MapType implements LogicalType
{
    public function __construct(private readonly MapKey $key, private readonly MapValue $value)
    {
    }

    public function isEqual(Type $type) : bool
    {
        if (!$type instanceof self) {
            return false;
        }

        return $this->key->toString() === $type->key()->toString() && $this->value->toString() === $type->value()->toString();
    }

    public function isValid(mixed $value) : bool
    {
        if (!\is_array($value)) {
            return false;
        }

        foreach ($value as $key => $item) {
            if (!$this->key->isValid($key)) {
                return false;
            }

            if (!$this->value->isValid($item)) {
                return false;
            }
        }

        return true;
    }

    public function key() : MapKey
    {
        return $this->key;
    }

    public function nullable() : bool
    {
        return false;
    }

    public function toString() : string
    {
        return 'map<' . $this->key->toString() . ', ' . $this->value->toString() . '>';
    }

    public function value() : MapValue
    {
        return $this->value;
    }
}
