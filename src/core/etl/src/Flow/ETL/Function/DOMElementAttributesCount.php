<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class DOMElementAttributesCount extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction|\DOMElement $domElement)
    {
    }

    public function eval(Row $row) : ?int
    {
        $domElement = (new Parameter($this->domElement))->asInstanceOf($row, \DOMElement::class);

        if ($domElement === null) {
            return null;
        }

        if (!$domElement->hasAttributes()) {
            return 0;
        }

        return $domElement->attributes->length;
    }
}
