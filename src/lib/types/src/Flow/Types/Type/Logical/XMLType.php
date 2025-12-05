<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use function Flow\Types\DSL\{type_string, type_xml};
use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type;

/**
 * @implements Type<\DOMDocument>
 */
final readonly class XMLType implements Type
{
    #[\Override]
    public function assert(mixed $value): \DOMDocument
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    #[\Override]
    public function cast(mixed $value): \DOMDocument
    {
        if ($this->isValid($value)) {
            return $value;
        }

        if (\is_string($value)) {
            $doc = new \DOMDocument();

            if (!@$doc->loadXML($value)) {
                throw new CastingException($value, type_xml());
            }

            return $doc;
        }

        try {
            $stringValue = type_string()->cast($value);

            $doc = new \DOMDocument();

            if (!@$doc->loadXML($stringValue)) {
                throw new CastingException($stringValue, $this);
            }

            return $doc;
        } catch (CastingException $e) {
            throw new CastingException($value, $this, $e);
        }
    }

    #[\Override]
    public function isValid(mixed $value): bool
    {
        if ($value instanceof \DOMDocument) {
            return true;
        }

        return false;
    }

    #[\Override]
    public function normalize(): array
    {
        return [
            'type' => 'xml',
        ];
    }

    #[\Override]
    public function toString(): string
    {
        return 'xml';
    }
}
