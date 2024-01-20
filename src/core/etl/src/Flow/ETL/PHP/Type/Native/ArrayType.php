<?php
declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Type;

final class ArrayType implements NativeType
{
    public function __construct(private readonly bool $empty = false, private readonly bool $nullable = false)
    {
    }

    public static function empty() : self
    {
        return new self(true);
    }

    public function isEqual(Type $type) : bool
    {
        return $type instanceof self && $this->empty === $type->empty;
    }

    public function isValid(mixed $value) : bool
    {
        return \is_array($value);
    }

    public function makeNullable(bool $nullable) : self
    {
        return new self($this->empty, $nullable);
    }

    public function merge(Type $type) : self
    {
        if ($type instanceof NullType) {
            return $this->makeNullable(true);
        }

        if (!$type instanceof self) {
            throw new InvalidArgumentException('Cannot merge different types, ' . $this->toString() . ' and ' . $type->toString());
        }

        return new self($this->empty || $type->empty, $this->nullable || $type->nullable());
    }

    public function nullable() : bool
    {
        return $this->nullable;
    }

    public function toString() : string
    {
        if ($this->empty) {
            return ($this->nullable ? '?' : '') . 'array<empty, empty>';
        }

        return ($this->nullable ? '?' : '') . 'array<mixed>';
    }
}
