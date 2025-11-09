<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\Types\DSL\{type_instance_of, type_list};
use Dom\HTMLElement;
use Flow\ETL\Row;

final class DOMElementAttributeValue extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|\DOMNode|HTMLElement $domElement,
        private readonly ScalarFunction|string $attribute,
    ) {
    }

    public function eval(Row $row) : ?string
    {
        $types = [
            type_instance_of(\DOMNode::class),
            type_list(type_instance_of(\DOMNode::class)),
        ];

        if (\class_exists('\Dom\HTMLElement')) {
            $types[] = type_instance_of(HTMLElement::class);
            $types[] = type_list(type_instance_of(HTMLElement::class));
        }

        $node = (new Parameter($this->domElement))->as(
            $row,
            ...$types
        );

        if ($node instanceof \DOMDocument) {
            $node = $node->documentElement;
        }

        if (\is_array($node) && \count($node)) {
            $node = \reset($node);
        }

        $attributeName = (new Parameter($this->attribute))->asString($row);

        if ($node === null || $attributeName === null) {
            return null;
        }

        if ((!$node instanceof \DOMNode && !$node instanceof HTMLElement) || !$node->hasAttributes()) {
            return null;
        }

        if (!$namedItem = $node->attributes->getNamedItem($attributeName)) {
            return null;
        }

        return $namedItem->nodeValue;
    }
}
