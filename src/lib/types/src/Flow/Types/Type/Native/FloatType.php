<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use DateInterval;
use DateTimeImmutable;
use DOMElement;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;

use function is_bool;
use function is_float;
use function is_numeric;

/**
 * @template T of float
 *
 * @implements Type<T>
 */
final readonly class FloatType implements Type
{
    public function assert(mixed $value): float
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    // @mago-ignore analysis:invalid-type-cast
    public function cast(mixed $value): float
    {
        if ($this->isValid($value)) {
            return $value;
        }

        if ($value instanceof DOMElement) {
            return (float) $value->nodeValue;
        }

        if ($value instanceof DateTimeImmutable) {
            // seconds with the fraction kept, where the integer casts floor to whole seconds
            return (float) $value->format('U.u');
        }

        if ($value instanceof DateInterval) {
            $reference = new DateTimeImmutable();
            $endTime = $reference->add($value);

            // the reference is a wall clock, so a sub-second interval straddles a second boundary
            // or not depending on when this runs - subtract in micros, then divide
            return ((float) $endTime->format('Uu') - (float) $reference->format('Uu')) / 1e6;
        }

        if (is_numeric($value)) {
            return (float) $value;
        }

        if (is_bool($value)) {
            return $value ? 1.0 : 0.0;
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value): bool
    {
        return is_float($value);
    }

    public function normalize(): array
    {
        return [
            'type' => 'float',
        ];
    }

    public function toString(): string
    {
        return 'float';
    }
}
