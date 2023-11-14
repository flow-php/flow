<?php
declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use Flow\ETL\PHP\Type\Type;

/**
 * @implements NativeType<array>
 */
final class NullType implements NativeType
{
    public function __serialize() : array
    {
        return [];
    }

    public function __unserialize(array $data) : void
    {

    }

    public function isEqual(Type $type) : bool
    {
        return $type instanceof self;
    }

    public function isValid(mixed $value) : bool
    {
        return null === $value;
    }

    public function nullable() : bool
    {
        return true;
    }

    public function toString() : string
    {
        return 'null';
    }
}
