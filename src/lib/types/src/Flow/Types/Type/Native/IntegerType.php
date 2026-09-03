<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use DateInterval;
use DateTimeImmutable;
use DOMElement;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Throwable;

use function is_bool;
use function is_int;
use function is_numeric;
use function is_object;

/**
 * @template T of int
 *
 * @implements Type<T>
 */
final readonly class IntegerType implements Type
{
    public function assert(mixed $value): int
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value): int
    {
        if ($this->isValid($value)) {
            return $value;
        }

        try {
            if ($value instanceof DOMElement) {
                return (int) $value->nodeValue;
            }

            if ($value instanceof DateTimeImmutable) {
                return (int) $value->format('Uu');
            }

            if ($value instanceof DateInterval) {
                $reference = new DateTimeImmutable();
                $endTime = $reference->add($value);

                return (int) $endTime->format('Uu') - (int) $reference->format('Uu');
            }

            if (is_object($value)) {
                throw new CastingException($value, $this);
            }

            if (is_numeric($value)) {
                return (int) $value;
            }

            if (is_bool($value)) {
                return $value ? 1 : 0;
            }

            throw new CastingException($value, $this);
        } catch (Throwable) {
            throw new CastingException($value, $this);
        }
    }

    public function isValid(mixed $value): bool
    {
        return is_int($value);
    }

    public function normalize(): array
    {
        return [
            'type' => 'integer',
        ];
    }

    public function toString(): string
    {
        return 'integer';
    }
}
