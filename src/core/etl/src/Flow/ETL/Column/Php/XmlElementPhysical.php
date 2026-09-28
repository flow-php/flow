<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

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
    ) {}

    public function toPhysical(mixed $value): mixed
    {
        if ($value instanceof DOMElement) {
            $xml = $value->ownerDocument?->saveXML($value);

            if ($xml === null || $xml === false) {
                throw new InvalidArgumentException('Floe failed to convert DOMElement to XML string');
            }

            return $xml;
        }

        /** @var \Dom\Element $value */
        // @mago-ignore analysis:non-existent-method
        // @mago-ignore analysis:mixed-assignment
        $markup = $value->ownerDocument?->saveXml($value);
        assert(is_string($markup));

        return $markup;
    }

    public function fromPhysical(mixed $physical): mixed
    {
        assert(is_string($physical));

        $element = type_instance_of(DOMDocument::class)->assert($this->document->fromPhysical(
            $physical,
        ))->documentElement;

        if ($element === null) {
            throw new InvalidArgumentException(sprintf('Floe failed to restore DOMElement from "%s"', $physical));
        }

        return $element;
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
