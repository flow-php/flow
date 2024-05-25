<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class DOMElementAttribute extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction $ref, private readonly string $attribute)
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

        $attributes = $value->attributes;

        if (!$namedItem = $attributes->getNamedItem($this->attribute)) {
            return null;
        }

        return $namedItem->nodeValue;
    }
}
