<?php
declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use Flow\ETL\PHP\Type\Type;

final class ArrayType implements NativeType
{
    public function __construct(private readonly bool $empty = false)
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

    public function toString() : string
    {
        if ($this->empty) {
            return 'array<empty, empty>';
        }

        return 'array<mixed>';
    }
}
