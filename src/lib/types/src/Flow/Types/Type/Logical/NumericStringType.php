<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type;

/**
 * @implements Type<numeric-string>
 */
final class NumericStringType implements Type
{
    #[\Override]
    public function assert(mixed $value): mixed
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    #[\Override]
    public function cast(mixed $value): string
    {
        if ($this->isValid($value)) {
            return $value;
        }

        if (\is_numeric($value)) {
            return (string) $value;
        }

        if ($value instanceof \Stringable) {
            if (\is_numeric((string) $value)) {
                return (string) $value;
            }
        }

        throw new CastingException($value, $this);
    }

    #[\Override]
    public function isValid(mixed $value): bool
    {
        return \is_string($value) && \is_numeric($value);
    }

    #[\Override]
    public function normalize(): array
    {
        return [
            'type' => 'numeric-string',
        ];
    }

    #[\Override]
    public function toString(): string
    {
        return 'numeric-string';
    }
}
