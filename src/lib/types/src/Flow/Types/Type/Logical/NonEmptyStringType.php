<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use function Flow\Types\DSL\dom_element_to_string;
use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type\Type;

/**
 * @implements Type<non-empty-string>
 */
final class NonEmptyStringType implements Type
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

        if (\is_bool($value)) {
            return $value ? 'true' : 'false';
        }

        try {
            if (\is_array($value)) {
                return $this->assert(\json_encode($value, JSON_THROW_ON_ERROR));
            }

            if ($value instanceof \DateTimeInterface) {
                return $this->assert($value->format(\DateTimeInterface::RFC3339));
            }

            if ($value instanceof \DateTimeZone) {
                return $this->assert($value->getName());
            }

            if ($value instanceof \Stringable) {
                return $this->assert((string) $value);
            }

            if ($value instanceof \DOMDocument) {
                return $this->assert($value->saveXML($value->documentElement) ?: '');
            }

            if ($value instanceof \DOMElement) {
                return $this->assert((string) dom_element_to_string($value));
            }

            return $this->assert((string) $value);
        } catch (\Throwable) {
            throw new CastingException($value, $this);
        }
    }

    public function isStringable(mixed $value) : bool
    {
        return \is_string($value) || (\is_object($value) && method_exists($value, '__toString')) || $value instanceof \Stringable;
    }

    public function isValid(mixed $value) : bool
    {
        return \is_string($value) && $value !== '';
    }

    public function normalize() : array
    {
        return [
            'type' => 'non_empty_string',
        ];
    }

    public function toString() : string
    {
        return 'non_empty_string';
    }
}
