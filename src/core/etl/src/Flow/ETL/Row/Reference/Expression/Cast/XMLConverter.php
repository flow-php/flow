<?php declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression\Cast;

final class XMLConverter
{
    /**
     * @param \DOMDocument $document
     *
     * @return array<mixed>
     */
    public function toArray(\DOMDocument $document) : array
    {
        $xmlArray = [];

        if ($document->hasChildNodes()) {
            foreach ($document->childNodes as $child) {
                $xmlArray[$child->nodeName] = $this->convertDOMElement($child);
            }
        }

        return $xmlArray;
    }

    /**
     * @psalm-suppress PossiblyNullArgument
     * @psalm-suppress PossiblyNullIterator
     */
    private function convertDOMElement(\DOMElement|\DOMNode $element) : array
    {
        $xmlArray = [];

        if ($element->hasAttributes()) {
            /**
             * @var \DOMAttr $attribute
             *
             * @phpstan-ignore-next-line
             */
            foreach ($element->attributes as $attribute) {
                $xmlArray['@attributes'][$attribute->name] = $attribute->value;
            }
        }

        foreach ($element->childNodes as $childNode) {
            if ($childNode->nodeType === XML_TEXT_NODE) {
                /** @phpstan-ignore-next-line  */
                if (\trim($childNode->nodeValue)) {
                    $xmlArray['@value'] = $childNode->nodeValue;
                }
            }

            if ($childNode->nodeType === XML_ELEMENT_NODE) {
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

    private function isElementCollection(\DOMElement|\DOMNode $element) : bool
    {
        if ($element->childNodes->count() <= 1) {
            return false;
        }

        $nodeNames = [];

        /** @var \DOMElement $childNode */
        foreach ($element->childNodes as $childNode) {
            if ($childNode->nodeType === XML_ELEMENT_NODE) {
                $nodeNames[] = $childNode->nodeName;
            }
        }

        if (\count($nodeNames) <= 1) {
            return false;
        }

        return \count(\array_unique($nodeNames)) === 1;
    }
}
