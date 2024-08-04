<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\XMLWriter;

use Flow\ETL\Adapter\XML\Abstraction\{XMLAttribute, XMLNode};
use Flow\ETL\Adapter\XML\XMLWriter;
use Flow\ETL\Exception\RuntimeException;

final class SimpleXMLWriter implements XMLWriter
{
    public function write(XMLNode $node) : string
    {
        $xml = new \SimpleXMLElement("<{$node->name}></{$node->name}>");
        $this->buildXMLNode($xml, $node);

        return \trim(\str_replace('<?xml version="1.0"?>', '', (string) $xml->asXML()));
    }

    private function addAttribute(\SimpleXMLElement $element, XMLAttribute $attribute) : void
    {
        $element->addAttribute($attribute->name, $attribute->value);
    }

    private function buildXMLNode(\SimpleXMLElement $element, XMLNode $node) : void
    {
        if ($node->hasValue()) {
            /** @phpstan-ignore-next-line */
            $element[0] = $node->value;
        }

        foreach ($node->attributes as $attribute) {
            $this->addAttribute($element, $attribute);
        }

        if ($node->hasChildren()) {
            foreach ($node->children as $child) {
                $childElement = $element->addChild($child->name);

                if ($childElement === null) {
                    throw new RuntimeException("Can't add child element to XML node");
                }

                $this->buildXMLNode($childElement, $child);
            }
        }
    }
}
