<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type;

/**
 * @implements Type<numeric-string>
 */
final class NumericStringType implements Type
{
    public function assert(mixed $value) : mixed
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : string
    {
        if ($this->isValid($value)) {
            return $value;
        }

        if (\is_numeric($value)) {
            return (string) $value;
        }

        if ($value instanceof \Stringable) {
            if (\is_numeric((string) $value)) {
                return (string) $value;
            }
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value) : bool
    {
        return \is_string($value) && \is_numeric($value);
    }

    public function normalize() : array
    {
        return [
            'type' => 'numeric-string',
        ];
    }

    public function toString() : string
    {
        return 'numeric-string';
    }
}
