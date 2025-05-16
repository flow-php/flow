<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use DateTimeInterface;
use Flow\ETL\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type\Type;

/**
 * @implements Type<DateTimeInterface>
 */
final readonly class DateTimeType implements Type
{
    public function assert(mixed $value) : \DateTimeInterface
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : \DateTimeInterface
    {
        if ($value instanceof \DateTimeImmutable) {
            return $value;
        }

        if ($value instanceof \DateTime) {
            return \DateTimeImmutable::createFromMutable($value);
        }

        if ($value instanceof \DOMElement) {
            $value = $value->nodeValue;
        }

        try {
            if (\is_string($value)) {
                return new \DateTimeImmutable($value);
            }

            if (\is_numeric($value)) {
                return new \DateTimeImmutable('@' . $value);
            }

            if (\is_bool($value)) {
                /* @phpstan-ignore-next-line */
                return new \DateTimeImmutable('@' . $value);
            }

            if ($value instanceof \DateInterval) {
                return (new \DateTimeImmutable('@0'))->add($value);

            }
        } catch (\Throwable) {
            throw new CastingException($value, $this);
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value) : bool
    {
        return $value instanceof \DateTimeInterface;
    }

    public function normalize() : array
    {
        return [
            'type' => 'datetime',
        ];
    }

    public function toString() : string
    {
        return 'datetime';
    }
}
