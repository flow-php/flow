<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type;
use Flow\Types\Value\HTMLDocument;

/**
 * @implements Type<HTMLDocument>
 */
final readonly class HTMLType implements Type
{
    public function assert(mixed $value) : HTMLDocument
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : HTMLDocument
    {
        if ($this->isValid($value)) {
            return $value;
        }

        if (\is_string($value)) {
            return new HTMLDocument($value);
        }

        if ($value instanceof \DOMDocument) {
            return new HTMLDocument($value);
        }

        if (\is_object($value) && \is_a($value, 'Dom\HTMLDocument')) {
            return new HTMLDocument($value);
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value) : bool
    {
        return $value instanceof HTMLDocument;
    }

    public function normalize() : array
    {
        return [
            'type' => 'html',
        ];
    }

    public function toString() : string
    {
        return 'html';
    }
}
