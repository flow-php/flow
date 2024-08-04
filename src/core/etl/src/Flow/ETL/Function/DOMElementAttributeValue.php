<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class DOMElementAttributeValue extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction $ref, private readonly ScalarFunction|string $attribute)
    {
    }

    public function eval(Row $row) : ?string
    {
        $value = $this->ref->eval($row);

        if ($value instanceof \DOMAttr) {
            return $value->nodeValue;
        }

        if (!$value instanceof \DOMElement) {
            return null;
        }

        if (!$value->hasAttributes()) {
            return null;
        }

        $attributeName = \is_string($this->attribute) ? $this->attribute : $this->attribute->eval($row);

        if (!\is_string($attributeName)) {
            return null;
        }

        $attributes = $value->attributes;

        if (!$namedItem = $attributes->getNamedItem($attributeName)) {
            return null;
        }

        return $namedItem->nodeValue;
    }
}
