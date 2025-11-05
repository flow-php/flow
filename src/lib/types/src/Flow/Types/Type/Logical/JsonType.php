<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use function Flow\Types\DSL\type_json;
use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type;

/**
 * @implements Type<string>
 */
final readonly class JsonType implements Type
{
    public function assert(mixed $value) : string
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : string
    {
        if ($this->isValid($value)) {
            return $value;
        }

        try {
            if (\is_scalar($value)) {
                throw new CastingException($value, type_json());
            }

            return \json_encode($value, \JSON_THROW_ON_ERROR);
        } catch (\Throwable) {
            throw new CastingException($value, $this);
        }
    }

    public function isValid(mixed $value) : bool
    {
        if (!\is_string($value)) {
            return false;
        }

        if ($value === '') {
            return false;
        }

        if ('{' !== $value[0] && '[' !== $value[0]) {
            return false;
        }

        if (
            !(\str_starts_with($value, '{') && \str_ends_with($value, '}'))
            && !(\str_starts_with($value, '[') && \str_ends_with($value, ']'))
        ) {
            return false;
        }

        return \json_validate($value);
    }

    public function normalize() : array
    {
        return [
            'type' => 'json',
        ];
    }

    public function toString() : string
    {
        return 'json';
    }
}
