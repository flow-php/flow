<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\{type_list, type_object};
use Flow\ETL\Row;

final class DOMElementAttributeValue extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|\DOMNode $domElement,
        private readonly ScalarFunction|string $attribute,
    ) {
    }

    public function eval(Row $row) : ?string
    {
        $node = (new Parameter($this->domElement))->as($row, type_object(\DOMNode::class), type_list(type_object(\DOMNode::class)));

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

        if (!$node->hasAttributes()) {
            return null;
        }

        $attributes = $node->attributes;

        if (!$namedItem = $attributes->getNamedItem($attributeName)) {
            return null;
        }

        return $namedItem->nodeValue;
    }
}
