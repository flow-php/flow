<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical;

use function Flow\ETL\DSL\type_time;
use Flow\ETL\Exception\{CastingException, InvalidTypeException};
use Flow\ETL\PHP\Type\Type;

/**
 * @implements Type<\DateInterval>
 */
final readonly class TimeType implements Type
{
    public function assert(mixed $value) : \DateInterval
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : \DateInterval
    {
        if ($value instanceof \DateTimeInterface) {
            return $value->diff(new \DateTimeImmutable($value->format('Y-m-d')), true);
        }

        if ($value instanceof \DateInterval) {
            return $value;
        }

        if ($value instanceof \DOMElement) {
            $value = $value->nodeValue;
        }

        try {
            if (\is_string($value)) {
                return new \DateInterval($value);
            }
        } catch (\Throwable) {
            throw new CastingException($value, type_time());
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value) : bool
    {
        return $value instanceof \DateInterval;
    }

    public function normalize() : array
    {
        return [
            'type' => 'time',
        ];
    }

    public function toString() : string
    {
        return 'time';
    }
}
