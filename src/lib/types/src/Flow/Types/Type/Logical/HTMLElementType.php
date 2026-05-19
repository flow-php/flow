<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical;

use Dom\HTMLDocument;
use Dom\HTMLElement;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;

use function Flow\Types\DSL\type_instance_of;
use function is_string;

use const LIBXML_HTML_NOIMPLIED;
use const LIBXML_NOERROR;

/**
 * @implements Type<HTMLElement>
 */
final readonly class HTMLElementType implements Type
{
    public function assert(mixed $value): HTMLElement
    {
        if ($this->isValid($value)) {
            return $value;
        }

        throw InvalidTypeException::value($value, $this);
    }

    public function cast(mixed $value): HTMLElement
    {
        if ($this->isValid($value)) {
            return $value;
        }

        if (is_string($value)) {
            $document = HTMLDocument::createFromString($value, LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR);

            return type_instance_of(HTMLElement::class)->assert($document->documentElement);
        }

        throw new CastingException($value, $this);
    }

    public function isValid(mixed $value): bool
    {
        return $value instanceof HTMLElement;
    }

    public function normalize(): array
    {
        return [
            'type' => 'html_element',
        ];
    }

    public function toString(): string
    {
        return 'html_element';
    }
}
