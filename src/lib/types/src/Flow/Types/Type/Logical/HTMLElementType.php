<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use function Flow\Types\DSL\type_instance_of;
use Dom\{HTMLDocument, HTMLElement};
use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Type;

/**
 * @implements Type<HTMLElement>
 */
final readonly class HTMLElementType implements Type
{
    #[\Override]
    public function assert(mixed $value) : HTMLElement
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    #[\Override]
    public function cast(mixed $value) : HTMLElement
    {
        if ($this->isValid($value)) {
            return $value;
        }

        if (\is_string($value)) {
            $document = HTMLDocument::createFromString($value, \LIBXML_HTML_NOIMPLIED | \LIBXML_NOERROR);

            return type_instance_of(HTMLElement::class)->assert($document->documentElement);
        }

        throw new CastingException($value, $this);
    }

    #[\Override]
    public function isValid(mixed $value) : bool
    {
        return $value instanceof HTMLElement;
    }

    #[\Override]
    public function normalize() : array
    {
        return [
            'type' => 'html_element',
        ];
    }

    #[\Override]
    public function toString() : string
    {
        return 'html_element';
    }
}
