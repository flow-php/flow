<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type;

/**
 * @implements Type<callable>
 */
final readonly class CallableType implements Type
{
    #[\Override]
    public function assert(mixed $value): callable
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    #[\Override]
    public function cast(mixed $value): callable
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw new CastingException($value, $this);
    }

    #[\Override]
    public function isValid(mixed $value): bool
    {
        return \is_callable($value);
    }

    #[\Override]
    public function normalize(): array
    {
        return [
            'type' => 'callable',
        ];
    }

    #[\Override]
    public function toString(): string
    {
        return 'callable';
    }
}
