<?php
declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use Flow\ETL\PHP\Type\Type;

final class ArrayType implements NativeType
{
    public function __construct(private readonly bool $empty = false)
    {
    }

    public function empty() : bool
    {
        return $this->empty;
    }

    public function isEqual(Type $type) : bool
    {
        return $type instanceof self;
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
