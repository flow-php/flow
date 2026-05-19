<?php

declare(strict_types=1);

namespace Flow\Types\Type\Logical\XML;

use DOMDocument;
use DOMElement;
use DOMNode;
use DOMText;

use function array_unique;
use function count;
use function trim;

final class XMLConverter
{
    /**
     * @param \DOMDocument $document
     *
     * @return array<mixed>
     */
    public function toArray(DOMDocument $document): array
    {
        $xmlArray = [];

        if ($document->hasChildNodes()) {
            foreach ($document->childNodes as $child) {
                if (!$child instanceof DOMElement) {
                    continue;
                }

                $xmlArray[$child->nodeName] = $this->convertDOMElement($child);
            }
        }

        return $xmlArray;
    }

    /**
     * @return array<string, mixed>
     */
    private function convertDOMElement(DOMElement|DOMNode $element): array
    {
        $xmlArray = [];

        if ($element->hasAttributes()) {
            // @mago-ignore analysis:possibly-null-iterator
            foreach ($element->attributes as $attribute) {
                $xmlArray['@attributes'][$attribute->name] = $attribute->value;
            }
        }

        foreach ($element->childNodes as $childNode) {
            if ($childNode instanceof DOMText) {
                if (trim((string) $childNode->nodeValue)) {
                    $xmlArray['@value'] = $childNode->nodeValue;
                }
            }

            if ($childNode instanceof DOMElement) {
                if ($this->isElementCollection($element)) {
                    /** @phpstan-ignore-next-line */
                    $xmlArray[$childNode->nodeName][] = $this->convertDOMElement($childNode);
                } else {
                    $xmlArray[$childNode->nodeName] = $this->convertDOMElement($childNode);
                }
            }
        }

        return $xmlArray;
    }

    private function isElementCollection(DOMElement|DOMNode $element): bool
    {
        if ($element->childNodes->count() <= 1) {
            return false;
        }

        $nodeNames = [];

        foreach ($element->childNodes as $childNode) {
            if ($childNode instanceof DOMElement) {
                $nodeNames[] = $childNode->nodeName;
            }
        }

        if (count($nodeNames) <= 1) {
            return false;
        }

        return count(array_unique($nodeNames)) === 1;
    }
}
