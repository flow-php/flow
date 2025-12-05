<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;

/**
 * @implements Type<null>
 */
final class NullType implements Type
{
    #[\Override]
    public function assert(mixed $value): null
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    #[\Override]
    public function cast(mixed $value): null
    {
        return null;
    }

    #[\Override]
    public function isValid(mixed $value): bool
    {
        return null === $value;
    }

    #[\Override]
    public function normalize(): array
    {
        return [
            'type' => 'null',
        ];
    }

    #[\Override]
    public function toString(): string
    {
        return 'null';
    }
}
