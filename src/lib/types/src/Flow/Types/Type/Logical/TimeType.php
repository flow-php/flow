<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use DateInterval;
use DateTimeImmutable;
use DateTimeInterface;
use DOMElement;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Throwable;

use function Flow\Types\DSL\type_time;
use function is_string;

/**
 * @template T of \DateInterval
 *
 * @implements Type<T>
 */
final readonly class TimeType implements Type
{
    public function assert(mixed $value): DateInterval
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value): DateInterval
    {
        if ($this->isValid($value)) {
            return $value;
        }

        if ($value instanceof DateTimeInterface) {
            return $value->diff(new DateTimeImmutable($value->format('Y-m-d')), true);
        }

        if ($value instanceof DOMElement) {
            $value = $value->nodeValue;
        }

        try {
            if (is_string($value)) {
                return new DateInterval($value);
            }
        } catch (Throwable) {
            throw new CastingException($value, type_time());
        }

        if ($value instanceof DateInterval) {
            throw new CastingException(
                $value,
                $this,
                reason: "Relative DateInterval (with months/years) can't be cast to time",
            );
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value): bool
    {
        return $value instanceof DateInterval && $value->y === 0 && $value->m === 0;
    }

    public function normalize(): array
    {
        return [
            'type' => 'time',
        ];
    }

    public function toString(): string
    {
        return 'time';
    }
}
