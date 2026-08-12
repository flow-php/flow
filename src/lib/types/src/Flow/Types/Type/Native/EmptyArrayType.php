<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Throwable;

use function Flow\Types\DSL\type_array;

/**
 * @template T of array{}
 *
 * @implements Type<T>
 */
final readonly class EmptyArrayType implements Type
{
    /**
     * @return array{}
     */
    public function assert(mixed $value): array
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    /**
     * @return array{}
     */
    public function cast(mixed $value): array
    {
        if ($this->isValid($value)) {
            return $value;
        }

        if (null === $value) {
            throw new CastingException($value, $this);
        }

        try {
            if ([] === type_array()->cast($value)) {
                return [];
            }
        } catch (Throwable) {
            throw new CastingException($value, $this);
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value): bool
    {
        return [] === $value;
    }

    public function normalize(): array
    {
        return [
            'type' => 'empty_array',
        ];
    }

    public function toString(): string
    {
        return 'array{}';
    }
}
