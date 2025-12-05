<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;

/**
 * @implements Type<object>
 */
final class ObjectType implements Type
{
    public function __construct() {}

    #[\Override]
    public function assert(mixed $value): mixed
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    #[\Override]
    public function cast(mixed $value): object
    {
        if ($this->isValid($value)) {
            return $value;
        }

        return (object) $value;
    }

    #[\Override]
    public function isValid(mixed $value): bool
    {
        return \is_object($value);
    }

    #[\Override]
    public function normalize(): array
    {
        return [
            'type' => 'object',
        ];
    }

    #[\Override]
    public function toString(): string
    {
        return 'object';
    }
}
