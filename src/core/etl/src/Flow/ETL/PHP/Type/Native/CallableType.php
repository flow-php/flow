<?php
declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use Flow\ETL\PHP\Type\Type;

final class CallableType implements NativeType
{
    public function isEqual(Type $type) : bool
    {
        return $type instanceof self;
    }

    public function isValid(mixed $value) : bool
    {
        return \is_callable($value);
    }

    public function toString() : string
    {
        return 'callable';
    }
}
