<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native;

use function Flow\ETL\DSL\dom_element_to_string;
use Flow\ETL\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type\Type;

/**
 * @implements Type<string>
 */
final readonly class StringType implements Type
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

        if (\is_array($value)) {
            return \json_encode($value, JSON_THROW_ON_ERROR);
        }

        if ($value instanceof \DateTimeInterface) {
            return $value->format(\DateTimeInterface::RFC3339);
        }

        if ($value instanceof \Stringable) {
            return (string) $value;
        }

        if ($value instanceof \DOMDocument) {
            return $value->saveXML($value->documentElement) ?: '';
        }

        if ($value instanceof \DOMElement) {
            return (string) dom_element_to_string($value);
        }

        try {
            return (string) $value;
            /* @phpstan-ignore-next-line */
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
        return \is_string($value);
    }

    public function normalize() : array
    {
        return [
            'type' => 'string',
        ];
    }

    public function toString() : string
    {
        return 'string';
    }
}
