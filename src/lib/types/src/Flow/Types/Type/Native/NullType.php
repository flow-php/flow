<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\Types\Exception\{InvalidTypeException};
use Flow\Types\Type;

/**
 * @implements Type<null>
 */
final class NullType implements Type
{
    public function assert(mixed $value) : null
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : null
    {
        return null;
    }

    public function isValid(mixed $value) : bool
    {
        return null === $value;
    }

    public function normalize() : array
    {
        return [
            'type' => 'null',
        ];
    }

    public function toString() : string
    {
        return 'null';
    }
}
