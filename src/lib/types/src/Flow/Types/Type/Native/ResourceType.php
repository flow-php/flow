<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;

use function is_resource;

/**
 * @template T of resource
 *
 * @implements Type<T>
 */
final readonly class ResourceType implements Type
{
    /**
     * @return resource
     */
    public function assert(mixed $value): mixed
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    /**
     * @return resource
     */
    public function cast(mixed $value): mixed
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value): bool
    {
        return is_resource($value);
    }

    public function normalize(): array
    {
        return [
            'type' => 'resource',
        ];
    }

    public function toString(): string
    {
        return 'resource';
    }
}
