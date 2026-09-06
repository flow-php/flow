<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use DateInterval;
use DateTime;
use DateTimeImmutable;
use DateTimeInterface;
use DOMElement;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Native\String\StringTemporalParts;
use Throwable;

use function is_bool;
use function is_numeric;
use function is_string;

/**
 * @template T of \DateTimeInterface
 *
 * @implements Type<T>
 */
final readonly class DateTimeType implements Type
{
    public function assert(mixed $value): DateTimeInterface
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value): DateTimeInterface
    {
        if ($value instanceof DateTimeImmutable) {
            return $value;
        }

        if ($value instanceof DateTime) {
            return DateTimeImmutable::createFromMutable($value);
        }

        if ($value instanceof DOMElement) {
            $value = $value->nodeValue;
        }

        try {
            if (is_string($value)) {
                $parts = StringTemporalParts::from($value);

                if (!$parts->isDate() && !$parts->isDateTime()) {
                    // DateTimeImmutable resolves '', 'now' and '+12' against the wall clock, so the
                    // same input written twice would produce two different values
                    throw new CastingException($value, $this, reason: 'value is not a calendar date');
                }

                return new DateTimeImmutable($value);
            }

            if (is_numeric($value)) {
                return new DateTimeImmutable('@' . $value);
            }

            if (is_bool($value)) {
                return new DateTimeImmutable('@' . (int) $value);
            }

            if ($value instanceof DateInterval) {
                return (new DateTimeImmutable('@0'))->add($value);
            }
        } catch (Throwable) {
            throw new CastingException($value, $this);
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value): bool
    {
        return $value instanceof DateTimeInterface;
    }

    public function normalize(): array
    {
        return [
            'type' => 'datetime',
        ];
    }

    public function toString(): string
    {
        return 'datetime';
    }
}
