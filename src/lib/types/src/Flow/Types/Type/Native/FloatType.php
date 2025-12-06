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
            /** @var numeric-string $nodeValue */
            $nodeValue = $value->nodeValue ?? '0';

            return (float) $nodeValue;
        }

        if ($value instanceof \DateTimeImmutable) {
            /** @var numeric-string $timestamp */
            $timestamp = $value->format('Uu');

            return (float) $timestamp;
        }

        if ($value instanceof \DateInterval) {
            $reference = new \DateTimeImmutable();
            $endTime = $reference->add($value);

            /** @var numeric-string $endTimestamp */
            $endTimestamp = $endTime->format('Uu');
            /** @var numeric-string $refTimestamp */
            $refTimestamp = $reference->format('Uu');

            return (float) $endTimestamp - (float) $refTimestamp;
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
