<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Native\{IntegerType, NullType, StringType};
use Flow\ETL\PHP\Type\{Type, TypeFactory};

/**
 * @implements Type<?array<array-key, mixed>>
 */
final readonly class MapType implements Type
{
    /**
     * @param Type<mixed> $value
     */
    public function __construct(private StringType|IntegerType $key, private Type $value, private bool $nullable = false)
    {
        if ($this->key->nullable()) {
            throw new InvalidArgumentException('Key cannot be nullable');
        }
    }

    public static function fromArray(array $data) : self
    {
        $keyType = TypeFactory::fromArray($data['key']);

        if (!$keyType instanceof StringType && !$keyType instanceof IntegerType) {
            throw new InvalidArgumentException('Map key must be string or integer');
        }

        return new self($keyType, TypeFactory::fromArray($data['value']), $data['nullable'] ?? false);
    }

    public function isComparableWith(Type $type) : bool
    {
        if ($type instanceof self) {
            return true;
        }

        if ($type instanceof NullType) {
            return true;
        }

        return false;
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
        if ($this->nullable && $value === null) {
            return true;
        }

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

    public function key() : StringType|IntegerType
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

        if (!$this->key->isEqual($type->key()) || !$this->value->isEqual($type->value())) {
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

    /**
     * @return Type<mixed>
     */
    public function value() : Type
    {
        return $this->value;
    }
}
