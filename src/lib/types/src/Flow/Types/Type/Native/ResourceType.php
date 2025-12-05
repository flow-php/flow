<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type;

/**
 * @implements Type<resource>
 */
final readonly class ResourceType implements Type
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
    public function cast(mixed $value): mixed
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw new CastingException($value, $this);
    }

    #[\Override]
    public function isValid(mixed $value): bool
    {
        return \is_resource($value);
    }

    #[\Override]
    public function normalize(): array
    {
        return [
            'type' => 'resource',
        ];
    }

    #[\Override]
    public function toString(): string
    {
        return 'resource';
    }
}
