<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type;
use function Flow\Types\DSL\type_instance_of;

/**
 * @implements Type<\DateTimeZone>
 */
final readonly class TimeZoneType implements Type
{
    #[\Override]
    public function assert(mixed $value) : \DateTimeZone
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    #[\Override]
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
                return type_instance_of(\DateTimeZone::class)->assert($value->getTimezone());
            }
        } catch (\Throwable) {
            throw new CastingException($value, $this);
        }

        throw new CastingException($value, $this);
    }

    #[\Override]
    public function isValid(mixed $value) : bool
    {
        return $value instanceof \DateTimeZone;
    }

    #[\Override]
    public function normalize() : array
    {
        return [
            'type' => 'timezone',
        ];
    }

    #[\Override]
    public function toString() : string
    {
        return 'timezone';
    }
}
