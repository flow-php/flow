<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class DOMElementAttributeValue extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|\DOMElement $domElement,
        private readonly ScalarFunction|string $attribute,
    ) {
    }

    public function eval(Row $row) : ?string
    {
        $domElement = (new Parameter($this->domElement))->asInstanceOf($row, \DOMElement::class);
        $attributeName = (new Parameter($this->attribute))->asString($row);

        if ($domElement === null || $attributeName === null) {
            return null;
        }

        if (!$domElement->hasAttributes()) {
            return null;
        }

        $attributes = $domElement->attributes;

        if (!$namedItem = $attributes->getNamedItem($attributeName)) {
            return null;
        }

        return $namedItem->nodeValue;
    }
}
