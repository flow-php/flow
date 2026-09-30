<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\XMLWriter;

use DOMDocument;
use DOMElement;
use Flow\ETL\Adapter\XML\Abstraction\XMLAttribute;
use Flow\ETL\Adapter\XML\Abstraction\XMLNode;
use Flow\ETL\Adapter\XML\XMLWriter;
use Flow\ETL\Exception\RuntimeException;

use function array_map;
use function Flow\Types\DSL\type_string;
use function substr;

final class DOMDocumentWriter implements XMLWriter
{
    public function attributes(string $name, array $values): array
    {
        return array_map(fn(?string $value): string => substr(
            $this->write(XMLNode::nested('x', new XMLAttribute($name, type_string()->cast($value)))),
            2,
            -2,
        ), $values);
    }

    public function elements(string $name, array $values): array
    {
        return array_map(fn(?string $value): string => $this->write(XMLNode::flatNode($name, $value ?? '')), $values);
    }

    public function write(XMLNode $node): string
    {
        $dom = new DOMDocument();
        $element = $this->createDOMElement($dom, $node);
        $dom->appendChild($element);

        $output = $dom->saveXML($element);

        if ($output === false) {
            throw new RuntimeException('Failed to write XML');
        }

        return $output;
    }

    private function createDOMElement(DOMDocument $dom, XMLNode $node): DOMElement
    {
        $element = $dom->createElement($node->name);

        if ($element === false) {
            throw new RuntimeException('Failed to create DOM element: ' . $node->name);
        }

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
