<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column\Physical;

use Dom\Element;
use Dom\XMLDocument;
use DOMDocument;
use DOMElement;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;

use function assert;
use function Flow\Types\DSL\type_instance_of;
use function is_string;
use function sprintf;

/**
 * A \Dom\Element (PHP 8.4) is stored by its markup too and reads back as the DOMElement XMLElementType::cast() builds
 * from a string.
 */
final readonly class XmlElementPhysical implements Physical
{
    public function __construct(
        private XmlDocumentPhysical $document = new XmlDocumentPhysical(),
        private ElementPosition $position = new ElementPosition(),
    ) {}

    public function toPhysical(mixed $value): mixed
    {
        /** @var DOMElement|\Dom\Element $value */
        $path = $this->position->path($value);

        if ($path === null) {
            return $this->canonical($this->rooted($value));
        }

        if ($value instanceof DOMElement) {
            $xml = $value->ownerDocument?->saveXML();

            if ($xml === null || $xml === false) {
                throw new InvalidArgumentException('Floe failed to convert DOMElement to XML string');
            }

            return $this->position->physical(
                $path,
                $this->canonical($value),
                (new XmlCharacterReferences())->decoded($xml),
            );
        }

        // @mago-ignore analysis:non-existent-method
        // @mago-ignore analysis:mixed-assignment
        $markup = $value->ownerDocument?->saveXml();
        assert(is_string($markup));

        return $this->position->physical(
            $path,
            $this->canonical($value),
            (new XmlCharacterReferences())->decoded($markup),
        );
    }

    public function fromPhysical(mixed $physical): mixed
    {
        assert(is_string($physical));

        $path = $this->position->pathOf($physical);
        $root = type_instance_of(DOMDocument::class)->assert($this->document->fromPhysical(
            $path === null ? $physical : $this->position->documentOf($physical),
        ))->documentElement;
        $element = $path === null ? $root : $this->position->element($root, $path);

        if ($element === null) {
            throw new InvalidArgumentException(sprintf('Floe failed to restore DOMElement from "%s"', $physical));
        }

        return $element;
    }

    public function markup(string $physical): string
    {
        return $this->position->markupOf($physical) ?? $physical;
    }

    /**
     * @param DOMElement|\Dom\Element $element
     */
    public function canonical(object $element): string
    {
        $markup = $element->C14N();

        if (!is_string($markup)) {
            throw new InvalidArgumentException('Floe failed to canonicalize XML element');
        }

        return $markup;
    }

    /**
     * @param DOMElement|\Dom\Element $element
     *
     * @return DOMElement|\Dom\Element
     */
    private function rooted(object $element): object
    {
        // C14N() of a node outside any document tree returns ''
        if ($element instanceof DOMElement) {
            $document = new DOMDocument();

            return type_instance_of(DOMElement::class)->assert($document->appendChild(type_instance_of(
                DOMElement::class,
            )->assert($document->importNode($element, true))));
        }

        // @mago-ignore analysis:unavailable-method
        $document = XMLDocument::createEmpty();

        return type_instance_of(Element::class)->assert($document->appendChild($document->importNode($element, true)));
    }

    public function fromPhysicalAll(array $physicals): array
    {
        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            $values[] = $physical === null ? null : $this->fromPhysical($physical);
        }

        return $values;
    }
}
