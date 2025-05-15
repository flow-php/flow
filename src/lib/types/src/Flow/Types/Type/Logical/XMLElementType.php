<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use DOMElement;
use Flow\ETL\Exception\{InvalidTypeException};
use Flow\Types\Type\Type;

/**
 * @implements Type<DOMElement>
 */
final readonly class XMLElementType implements Type
{
    public function assert(mixed $value) : \DOMElement
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value) : \DOMElement
    {
        throw new \RuntimeException('not implemented');
    }

    public function isValid(mixed $value) : bool
    {
        if ($value instanceof \DOMElement) {
            return true;
        }

        return false;
    }

    public function normalize() : array
    {
        return [
            'type' => 'xml_element',
        ];
    }

    public function toString() : string
    {
        return 'xml_element';
    }
}
