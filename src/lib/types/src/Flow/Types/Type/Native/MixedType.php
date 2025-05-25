<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\Types\Type;

/**
 * @implements Type<mixed>
 */
final class MixedType implements Type
{
    public function assert(mixed $value) : mixed
    {
        return $value;
    }

    public function cast(mixed $value) : mixed
    {
        return $value;
    }

    public function isValid(mixed $value) : bool
    {
        return true;
    }

    public function normalize() : array
    {
        return [
            'type' => 'mixed',
        ];
    }

    public function toString() : string
    {
        return 'mixed';
    }
}
