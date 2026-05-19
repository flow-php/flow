<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use DOMDocument;
use DOMElement;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;

use function Flow\Types\DSL\type_instance_of;
use function is_string;

/**
 * @implements Type<\DOMElement>
 */
final readonly class XMLElementType implements Type
{
    public function assert(mixed $value): DOMElement
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value): DOMElement
    {
        if ($this->isValid($value)) {
            return $value;
        }

        if (is_string($value)) {
            $dom = new DOMDocument();
            $dom->loadXML($value);

            return type_instance_of(DOMElement::class)->assert($dom->documentElement);
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value): bool
    {
        return $value instanceof DOMElement;
    }

    public function normalize(): array
    {
        return [
            'type' => 'xml_element',
        ];
    }

    public function toString(): string
    {
        return 'xml_element';
    }
}
