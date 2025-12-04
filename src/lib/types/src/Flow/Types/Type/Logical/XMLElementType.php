<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use function Flow\Types\DSL\type_instance_of;
use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type;

/**
 * @implements Type<\DOMElement>
 */
final readonly class XMLElementType implements Type
{
    #[\Override]
    public function assert(mixed $value) : \DOMElement
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    #[\Override]
    public function cast(mixed $value) : \DOMElement
    {
        if ($this->isValid($value)) {
            return $value;
        }

        if (\is_string($value)) {
            $dom = new \DOMDocument();
            $dom->loadXML($value);

            return type_instance_of(\DOMElement::class)->assert($dom->documentElement);
        }

        throw new CastingException($value, $this);
    }

    #[\Override]
    public function isValid(mixed $value) : bool
    {
        return $value instanceof \DOMElement;
    }

    #[\Override]
    public function normalize() : array
    {
        return [
            'type' => 'xml_element',
        ];
    }

    #[\Override]
    public function toString() : string
    {
        return 'xml_element';
    }
}
