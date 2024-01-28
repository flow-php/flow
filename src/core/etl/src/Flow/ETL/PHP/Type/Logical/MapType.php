<?php declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Native\NullType;
use Flow\ETL\PHP\Type\Type;

final class MapType implements LogicalType
{
    public function __construct(private readonly MapKey $key, private readonly MapValue $value, private readonly bool $nullable = false)
    {
    }

    public static function fromArray(array $data) : self
    {
        return new self(MapKey::fromArray($data['key']), MapValue::fromArray($data['value']), $data['nullable'] ?? false);
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

    public function makeNullable(bool $nullable) : self
    {
        return new self($this->key, $this->value, $nullable);
    }

    public function merge(Type $type) : self
    {
        if ($type instanceof NullType) {
            return $this->makeNullable(true);
        }

        if (!$type instanceof self) {
            throw new InvalidArgumentException('Cannot merge different types, ' . $this->toString() . ' and ' . $type->toString());
        }

        if (!$this->key->type()->isEqual($type->key()->type()) || !$this->value->type()->isEqual($type->value()->type())) {
            throw new InvalidArgumentException('Cannot merge different types, ' . $this->toString() . ' and ' . $type->toString());
        }

        return new self($this->key, $this->value, $this->nullable || $type->nullable());
    }

    public function normalize() : array
    {
        return [
            'type' => 'map',
            'key' => $this->key->normalize(),
            'value' => $this->value->normalize(),
            'nullable' => $this->nullable,
        ];
    }

    public function nullable() : bool
    {
        return $this->nullable;
    }

    public function toString() : string
    {
        return ($this->nullable ? '?' : '') . 'map<' . $this->key->toString() . ', ' . $this->value->toString() . '>';
    }

    public function value() : MapValue
    {
        return $this->value;
    }
}
