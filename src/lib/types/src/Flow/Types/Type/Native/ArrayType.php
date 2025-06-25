<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\Types\Exception\{CastingException};
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\XML\XMLConverter;

/**
 * @implements Type<array>
 */
final readonly class ArrayType implements Type
{
    /**
     * @return array<array-key, mixed>
     */
    public function assert(mixed $value) : array
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    /**
     * @return array<array-key, mixed>
     */
    public function cast(mixed $value) : array
    {
        if ($this->isValid($value)) {
            return $value;
        }

        try {
            if (\is_string($value) && (\str_starts_with($value, '{') || \str_starts_with($value, '['))) {
                $decoded = \json_decode($value, true, 512, \JSON_THROW_ON_ERROR);

                return \is_array($decoded) ? $decoded : throw new CastingException($value, $this);
            }

            if ($value instanceof \DOMDocument) {
                return (new XMLConverter())->toArray($value);
            }

            if (\is_object($value)) {
                $encoded = \json_decode(\json_encode($value, \JSON_THROW_ON_ERROR), true, 512, \JSON_THROW_ON_ERROR);

                return \is_array($encoded) ? $encoded : throw new CastingException($value, $this);
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
