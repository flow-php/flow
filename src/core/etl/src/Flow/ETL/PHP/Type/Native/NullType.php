<?php
declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Native;

use Flow\ETL\PHP\Type\Type;

final class NullType implements NativeType
{
    public static function fromArray(array $data) : self
    {
        return new self();
    }

    public function isEqual(Type $type) : bool
    {
        return $type instanceof self;
    }

    public function isValid(mixed $value) : bool
    {
        return null === $value;
    }

    public function makeNullable(bool $nullable) : self
    {
        return $this;
    }

    public function merge(Type $type) : self
    {
        /** @phpstan-ignore-next-line  */
        return $type->makeNullable(true);
    }

    public function normalize() : array
    {
        return [
            'type' => 'null',
        ];
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
