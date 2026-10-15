<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Dom\XMLDocument;
use DOMDocument;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;

use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_xml;
use function is_string;

/**
 * @implements Type<\DOMDocument|XMLDocument>
 */
final readonly class XMLType implements Type
{
    public function assert(mixed $value): DOMDocument|XMLDocument
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value): DOMDocument|XMLDocument
    {
        if ($this->isValid($value)) {
            return $value;
        }

        if (is_string($value)) {
            $doc = new DOMDocument();

            if (!@$doc->loadXML($value)) {
                throw new CastingException($value, type_xml());
            }

            return $doc;
        }

        try {
            $stringValue = type_string()->cast($value);

            $doc = new DOMDocument();

            if (!@$doc->loadXML($stringValue)) {
                throw new CastingException($stringValue, $this);
            }

            return $doc;
        } catch (CastingException $e) {
            throw new CastingException($value, $this, $e);
        }
    }

    public function isValid(mixed $value): bool
    {
        return $value instanceof DOMDocument || $value instanceof XMLDocument;
    }

    public function normalize(): array
    {
        return [
            'type' => 'xml',
        ];
    }

    public function toString(): string
    {
        return 'xml';
    }
}
