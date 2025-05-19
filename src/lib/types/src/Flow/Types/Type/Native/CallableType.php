<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\Types\Exception\{CastingException};
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type\Type;

/**
 * @implements Type<callable>
 */
final readonly class CallableType implements Type
{
    public function assert(mixed $value) : callable
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : callable
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value) : bool
    {
        return \is_callable($value);
    }

    public function normalize() : array
    {
        return [
            'type' => 'callable',
        ];
    }

    public function toString() : string
    {
        return 'callable';
    }
}
