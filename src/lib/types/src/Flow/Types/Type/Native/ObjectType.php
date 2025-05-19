<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\Types\Exception\{InvalidTypeException};
use Flow\Types\Type\Type;

/**
 * @implements Type<object>
 */
final class ObjectType implements Type
{
    public function __construct()
    {
    }

    public function assert(mixed $value) : mixed
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : object
    {
        if ($this->isValid($value)) {
            return $value;
        }

        return (object) $value;
    }

    public function isValid(mixed $value) : bool
    {
        return \is_object($value);
    }

    public function normalize() : array
    {
        return [
            'type' => 'object',
        ];
    }

    public function toString() : string
    {
        return 'object';
    }
}
