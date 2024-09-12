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
        $domElement = Parameter::oneOf(
            (new Parameter($this->domElement))->asInstanceOf($row, \DOMElement::class),
            (new Parameter($this->domElement))->asInstanceOf($row, \DOMDocument::class),
            (new Parameter($this->domElement))->asListOfObjects($row, \DOMElement::class),
        );

        if ($domElement instanceof \DOMDocument) {
            $domElement = $domElement->documentElement;
        }

        if (\is_array($domElement) && \count($domElement)) {
            $domElement = \reset($domElement);
        }

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
