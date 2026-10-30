<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\IO\Loader\XMLWriter;

use DOMDocument;
use DOMElement;
use Flow\ETL\Adapter\XML\IO\Loader\XMLAttribute;
use Flow\ETL\Adapter\XML\IO\Loader\XMLNode;
use Flow\ETL\Adapter\XML\IO\Loader\XMLWriter;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\RuntimeException;
use SensitiveParameter;

use function array_map;
use function Flow\Types\DSL\type_string;
use function substr;

final class DOMDocumentWriter implements Describable, XMLWriter
{
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self();
    }

    public function describe(): ?Description
    {
        return new Description(self::class, [], extensions: ['dom', 'xml', 'xmlreader', 'xmlwriter']);
    }

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
