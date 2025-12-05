<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use function Flow\Types\DSL\type_time;
use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type;

/**
 * @implements Type<\DateInterval>
 */
final readonly class TimeType implements Type
{
    #[\Override]
    public function assert(mixed $value): \DateInterval
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    #[\Override]
    public function cast(mixed $value): \DateInterval
    {
        if ($this->isValid($value)) {
            return $value;
        }

        if ($value instanceof \DateTimeInterface) {
            return $value->diff(new \DateTimeImmutable($value->format('Y-m-d')), true);
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

    #[\Override]
    public function isValid(mixed $value): bool
    {
        return $value instanceof \DateInterval;
    }

    #[\Override]
    public function normalize(): array
    {
        return [
            'type' => 'time',
        ];
    }

    #[\Override]
    public function toString(): string
    {
        return 'time';
    }
}
