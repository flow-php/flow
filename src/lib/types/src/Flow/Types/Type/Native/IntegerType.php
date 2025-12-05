<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type;

/**
 * @implements Type<int>
 */
final readonly class IntegerType implements Type
{
    #[\Override]
    public function assert(mixed $value): int
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    #[\Override]
    public function cast(mixed $value): int
    {
        if ($this->isValid($value)) {
            return $value;
        }

        try {
            if ($value instanceof \DOMElement) {
                return (int) $value->nodeValue;
            }

            if ($value instanceof \DateTimeImmutable) {
                return (int) $value->format('Uu');
            }

            if ($value instanceof \DateInterval) {
                $reference = new \DateTimeImmutable();
                $endTime = $reference->add($value);

                return (int) $endTime->format('Uu') - (int) $reference->format('Uu');
            }

            if (\is_object($value)) {
                throw new CastingException($value, $this);
            }

            if (\is_scalar($value) || null === $value || \is_array($value)) {
                return (int) $value;
            }

            throw new CastingException($value, $this);
        } catch (\Throwable) {
            throw new CastingException($value, $this);
        }
    }

    #[\Override]
    public function isValid(mixed $value): bool
    {
        return \is_int($value);
    }

    #[\Override]
    public function normalize(): array
    {
        return [
            'type' => 'integer',
        ];
    }

    #[\Override]
    public function toString(): string
    {
        return 'integer';
    }
}
