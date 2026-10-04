<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

use DOMDocument;
use DOMElement;
use Flow\ETL\Exception\InvalidArgumentException;

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
        if ($value instanceof DOMElement) {
            $path = $this->position->path($value);
            $xml = $value->ownerDocument?->saveXML($path === null ? $value : null);

            if ($xml === null || $xml === false) {
                throw new InvalidArgumentException('Floe failed to convert DOMElement to XML string');
            }

            $xml = (new XmlCharacterReferences())->decoded($xml);

            return $path === null ? $xml : $this->position->physical($path, $this->canonical($value), $xml);
        }

        /** @var \Dom\Element $value */
        $path = $this->position->path($value);
        // @mago-ignore analysis:non-existent-method
        // @mago-ignore analysis:mixed-assignment
        $markup = $value->ownerDocument?->saveXml($path === null ? $value : null);
        assert(is_string($markup));
        $markup = (new XmlCharacterReferences())->decoded($markup);

        return $path === null ? $markup : $this->position->physical($path, $this->canonical($value), $markup);
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
        return (
            $this->position->markupOf($physical) ?? $this->canonical(type_instance_of(
                DOMElement::class,
            )->assert($this->fromPhysical($physical)))
        );
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
