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
use Throwable;

use function is_bool;
use function is_numeric;
use function is_string;

/**
 * @template T of \DateTimeInterface
 *
 * @implements Type<T>
 */
final readonly class DateType implements Type
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
        if ($this->isValid($value)) {
            return $value;
        }

        if ($value instanceof DOMElement) {
            $value = $value->nodeValue;
        }

        try {
            if ($value instanceof DateTimeImmutable || $value instanceof DateTime) {
                return $value->setTime(0, 0, 0, 0);
            }

            if (is_string($value)) {
                return (new DateTimeImmutable($value))->setTime(0, 0, 0, 0);
            }

            if (is_numeric($value)) {
                return (new DateTimeImmutable('@' . $value))->setTime(0, 0, 0, 0);
            }

            if (is_bool($value)) {
                return (new DateTimeImmutable('@' . (int) $value))->setTime(0, 0, 0, 0);
            }

            if ($value instanceof DateInterval) {
                return (new DateTimeImmutable('@0'))->add($value)->setTime(0, 0, 0, 0);
            }
        } catch (Throwable) {
            throw new CastingException($value, $this);
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value): bool
    {
        return $value instanceof DateTimeInterface && $value->format('H:i:s.u') === '00:00:00.000000';
    }

    public function normalize(): array
    {
        return [
            'type' => 'date',
        ];
    }

    public function toString(): string
    {
        return 'date';
    }
}
