<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Type;

final readonly class FloatType implements NativeType
{
    public function __construct(private readonly bool $nullable = false, public readonly int $precision = 6)
    {
    }

    public static function fromArray(array $data) : Type
    {
        $nullable = $data['nullable'] ?? false;
        $precision = $data['precision'] ?? 6;

        return new self($nullable, $precision);
    }

    public function isComparableWith(Type $type) : bool
    {
        if ($type instanceof self) {
            return true;
        }

        if ($type instanceof NullType) {
            return true;
        }

        if ($type instanceof IntegerType) {
            return true;
        }

        return false;
    }

    public function isEqual(Type $type) : bool
    {
        return $type instanceof self && $this->nullable === $type->nullable && $this->precision === $type->precision;
    }

    public function isValid(mixed $value) : bool
    {
        if ($this->nullable && $value === null) {
            return true;
        }

        return \is_float($value);
    }

    public function makeNullable(bool $nullable) : Type
    {
        return new self($nullable, $this->precision);
    }

    public function merge(Type $type) : Type
    {
        if ($type instanceof NullType) {
            return $this->makeNullable(true);
        }

        if (!$type instanceof self) {
            throw new InvalidArgumentException('Cannot merge different types, ' . $this->toString() . ' and ' . $type->toString());
        }

        $precision = min($type->precision, $this->precision);

        return new self($this->nullable || $type->nullable(), $precision);
    }

    public function normalize() : array
    {
        return [
            'type' => 'float',
            'nullable' => $this->nullable,
            'precision' => $this->precision,
        ];
    }

    public function nullable() : bool
    {
        return $this->nullable;
    }

    public function toString() : string
    {
        return ($this->nullable ? '?' : '') . 'float';
    }
}
