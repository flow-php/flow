<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\XMLWriter;

use Flow\ETL\Adapter\XML\Abstraction\XMLNode;
use Flow\ETL\Adapter\XML\XMLWriter;
use Flow\ETL\Exception\RuntimeException;

final class DOMDocumentWriter implements XMLWriter
{
    public function write(XMLNode $node) : string
    {
        $dom = new \DOMDocument();
        $element = $this->createDOMElement($dom, $node);
        $dom->appendChild($element);

        $output = $dom->saveXML($element);

        if ($output === false) {
            throw new RuntimeException('Failed to write XML');
        }

        return $output;
    }

    private function createDOMElement(\DOMDocument $dom, XMLNode $node) : \DOMElement
    {
        $element = $dom->createElement($node->name);

        if ($node->hasValue()) {
            $element->appendChild($dom->createTextNode((string) $node->value));
        }

        foreach ($node->attributes as $attribute) {
            $element->setAttribute($attribute->name, $attribute->value);
        }

        if ($node->hasChildren()) {
            foreach ($node->children as $child) {
                $childElement = $this->createDOMElement($dom, $child);
                $element->appendChild($childElement);
            }
        }

        return $element;
    }
}
