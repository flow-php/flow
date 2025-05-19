<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\Types\Exception\{CastingException};
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type\Logical\XML\XMLConverter;
use Flow\Types\Type\Type;

/**
 * @implements Type<array>
 */
final readonly class ArrayType implements Type
{
    public function assert(mixed $value) : array
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : array
    {
        if ($this->isValid($value)) {
            return $value;
        }

        try {
            if (\is_string($value) && (\str_starts_with($value, '{') || \str_starts_with($value, '['))) {
                return \json_decode($value, true, 512, \JSON_THROW_ON_ERROR);
            }

            if ($value instanceof \DOMDocument) {
                return (new XMLConverter())->toArray($value);
            }

            if (\is_object($value)) {
                return \json_decode(\json_encode($value, \JSON_THROW_ON_ERROR), true, 512, \JSON_THROW_ON_ERROR);
            }

            return (array) $value;
        } catch (\Throwable) {
            throw new CastingException($value, $this);
        }
    }

    public function isValid(mixed $value) : bool
    {
        if (!\is_array($value)) {
            return false;
        }

        return true;
    }

    public function normalize() : array
    {
        return [
            'type' => 'array',
        ];
    }

    public function toString() : string
    {
        return 'array<mixed>';
    }
}
