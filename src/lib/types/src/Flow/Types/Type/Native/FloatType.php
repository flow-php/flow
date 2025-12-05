<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type;

/**
 * @implements Type<float>
 */
final readonly class FloatType implements Type
{
    #[\Override]
    public function assert(mixed $value): float
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    #[\Override]
    public function cast(mixed $value): float
    {
        if ($this->isValid($value)) {
            return $value;
        }

        if ($value instanceof \DOMElement) {
            return (float) $value->nodeValue;
        }

        if ($value instanceof \DateTimeImmutable) {
            return (float) $value->format('Uu');
        }

        if ($value instanceof \DateInterval) {
            $reference = new \DateTimeImmutable();
            $endTime = $reference->add($value);

            return (float) $endTime->format('Uu') - (float) $reference->format('Uu');
        }

        if (\is_scalar($value) || null === $value || \is_array($value)) {
            return (float) $value;
        }

        throw new CastingException($value, $this);
    }

    #[\Override]
    public function isValid(mixed $value): bool
    {
        return \is_float($value);
    }

    #[\Override]
    public function normalize(): array
    {
        return [
            'type' => 'float',
        ];
    }

    #[\Override]
    public function toString(): string
    {
        return 'float';
    }
}
