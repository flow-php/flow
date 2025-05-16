<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use Flow\ETL\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type\Type;

/**
 * @implements Type<boolean>
 */
final readonly class BooleanType implements Type
{
    public function assert(mixed $value) : bool
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : bool
    {
        if ($this->isValid($value)) {
            return $value;
        }

        if ($value instanceof \DOMElement) {
            $value = $value->nodeValue;
        }

        if (\is_string($value)) {
            if (\in_array(\mb_strtolower($value), ['true', '1', 'yes', 'on'], true)) {
                return true;
            }

            if (\in_array(\mb_strtolower($value), ['false', '0', 'no', 'off'], true)) {
                return false;
            }
        }

        try {
            return (bool) $value;
            /* @phpstan-ignore-next-line */
        } catch (\Throwable) {
            throw new CastingException($value, $this);
        }
    }

    public function isValid(mixed $value) : bool
    {
        return \is_bool($value);
    }

    public function normalize() : array
    {
        return [
            'type' => 'boolean',
        ];
    }

    public function toString() : string
    {
        return 'boolean';
    }
}
