<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type;

/**
 * @implements Type<\DateTimeZone>
 */
final readonly class TimeZoneType implements Type
{
    public function assert(mixed $value) : \DateTimeZone
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : \DateTimeZone
    {
        if ($this->isValid($value)) {
            return $value;
        }

        if ($value instanceof \DOMElement) {
            $value = $value->nodeValue;
        }

        try {
            if (\is_string($value)) {
                return new \DateTimeZone($value);
            }

            if ($value instanceof \DateTimeInterface) {
                return $value->getTimezone();
            }
        } catch (\Throwable) {
            throw new CastingException($value, $this);
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value) : bool
    {
        return $value instanceof \DateTimeZone;
    }

    public function normalize() : array
    {
        return [
            'type' => 'timezone',
        ];
    }

    public function toString() : string
    {
        return 'timezone';
    }
}
