<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Flow\Types\Exception\{CastingException, InvalidArgumentException, InvalidTypeException};
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

        if (!is_string($value) && !is_object($value)) {
            throw new CastingException($value, $this);
        }

        try {
            return new HTMLDocument($value);
        } catch (InvalidArgumentException $e) {
            throw new CastingException($value, $this, $e);
        }
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
